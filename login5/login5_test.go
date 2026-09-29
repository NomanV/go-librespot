package login5

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	pb "github.com/devgianlu/go-librespot/proto/spotify/login5/v3"
	credentialspb "github.com/devgianlu/go-librespot/proto/spotify/login5/v3/credentials"
	"google.golang.org/protobuf/proto"
)

// replyFunc picks the status and body the fake login5 answers a request with.
type replyFunc func(req *pb.LoginRequest) (int, []byte)

// fakeLogin5 is a login5 stand-in whose answer the test swaps at will.
type fakeLogin5 struct {
	t *testing.T

	mu       sync.Mutex
	reply    replyFunc
	requests []*pb.LoginRequest
}

func (f *fakeLogin5) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v3/login" {
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		f.t.Errorf("failed reading request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var req pb.LoginRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		f.t.Errorf("failed unmarshalling LoginRequest: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.requests = append(f.requests, &req)
	reply := f.reply
	f.mu.Unlock()

	status, resp := reply(&req)
	if status == http.StatusOK {
		w.Header().Set("Content-Type", "application/x-protobuf")
	} else {
		w.Header().Set("Content-Type", "text/plain")
	}
	w.WriteHeader(status)
	_, _ = w.Write(resp)
}

func (f *fakeLogin5) setReply(reply replyFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reply = reply
}

func (f *fakeLogin5) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeLogin5) lastRequest() *pb.LoginRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return nil
	}
	return f.requests[len(f.requests)-1]
}

// okReply answers every request with a LoginOk carrying the given token.
func okReply(username, token string, expiresIn int32) replyFunc {
	return func(*pb.LoginRequest) (int, []byte) {
		body, err := proto.Marshal(&pb.LoginResponse{
			Response: &pb.LoginResponse_Ok{Ok: &pb.LoginOk{
				Username:             username,
				AccessToken:          token,
				StoredCredential:     []byte("stored-" + username),
				AccessTokenExpiresIn: expiresIn,
			}},
		})
		if err != nil {
			panic(err)
		}
		return http.StatusOK, body
	}
}

// rawReply answers every request with a verbatim status and body.
func rawReply(status int, body string) replyFunc {
	return func(*pb.LoginRequest) (int, []byte) {
		return status, []byte(body)
	}
}

func newTestLogin5(t *testing.T, reply replyFunc) (*Login5, *fakeLogin5) {
	t.Helper()

	fake := &fakeLogin5{t: t, reply: reply}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	c := newLogin5WithBaseURL(&librespot.NullLogger{}, srv.Client(), srv.URL, "test-device", "test-client-token")
	return c, fake
}

func storedCredential() *credentialspb.StoredCredential {
	return &credentialspb.StoredCredential{Username: "alice", Data: []byte("blob")}
}

func TestLoginHTTPErrorIsReported(t *testing.T) {
	c, _ := newTestLogin5(t, rawReply(http.StatusServiceUnavailable, "no healthy upstream"))

	err := c.Login(context.Background(), storedCredential())
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"login5 HTTP 503", "no healthy upstream"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "wire-format") {
		t.Errorf("error %q should not be a protobuf parse error", err)
	}
}

func TestLoginEmptyResponseIsReported(t *testing.T) {
	for name, body := range map[string]string{
		"zero bytes":         "",
		"unknown field only": "\x40\x00", // field 8, varint 0: parses, sets nothing
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestLogin5(t, rawReply(http.StatusOK, body))

			err := c.Login(context.Background(), storedCredential())
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), "empty response") {
				t.Errorf("error %q does not contain %q", err, "empty response")
			}
			if strings.Contains(err.Error(), "UNKNOWN_ERROR") {
				t.Errorf("error %q should not be a LoginError", err)
			}
		})
	}
}

func TestLoginErrorResponseIsStillALoginError(t *testing.T) {
	body, err := proto.Marshal(&pb.LoginResponse{
		Response: &pb.LoginResponse_Error{Error: pb.LoginError_INVALID_CREDENTIALS},
	})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := newTestLogin5(t, rawReply(http.StatusOK, string(body)))

	err = c.Login(context.Background(), storedCredential())
	var loginErr *LoginError
	if !errors.As(err, &loginErr) {
		t.Fatalf("expected a *LoginError, got %v", err)
	}
	if loginErr.Code != pb.LoginError_INVALID_CREDENTIALS {
		t.Errorf("got code %v, want INVALID_CREDENTIALS", loginErr.Code)
	}
}

func TestAccessTokenFastPathDoesNotHitServer(t *testing.T) {
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 3600))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	fake.setReply(rawReply(http.StatusServiceUnavailable, "no healthy upstream"))

	token, err := c.AccessToken()(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "token-a" {
		t.Errorf("got token %q, want token-a", token)
	}
	if n := fake.requestCount(); n != 1 {
		t.Errorf("server saw %d requests, want 1 (the login only)", n)
	}
}

func TestAccessTokenInsideRefreshWindowKeepsOldTokenOnFailure(t *testing.T) {
	// 10 minutes left is inside the 15 minute refresh window
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 600))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	fake.setReply(rawReply(http.StatusServiceUnavailable, "no healthy upstream"))

	token, err := c.AccessToken()(context.Background(), false)
	if err != nil {
		t.Fatalf("expected the old token with no error, got error: %v", err)
	}
	if token != "token-a" {
		t.Errorf("got token %q, want the old token-a", token)
	}
	if n := fake.requestCount(); n != 2 {
		t.Errorf("server saw %d requests, want 2 (login + one renewal attempt)", n)
	}

	// the renewal must have used the stored credential from the first login
	if sc := fake.lastRequest().GetStoredCredential(); sc == nil {
		t.Errorf("renewal did not use a stored credential: %v", fake.lastRequest())
	} else if sc.Username != "alice" || string(sc.Data) != "stored-alice" {
		t.Errorf("renewal used stored credential %q/%q", sc.Username, sc.Data)
	}

	// once login5 is back, the next call inside the window picks up the new token
	fake.setReply(okReply("alice", "token-b", 3600))
	token, err = c.AccessToken()(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "token-b" {
		t.Errorf("got token %q, want the renewed token-b", token)
	}
}

func TestAccessTokenInsideRefreshWindowForceStillFails(t *testing.T) {
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 600))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	fake.setReply(rawReply(http.StatusServiceUnavailable, "no healthy upstream"))

	if _, err := c.AccessToken()(context.Background(), true); err == nil {
		t.Fatal("expected an error when forcing a renewal that fails")
	}
}

func TestAccessTokenAfterExpiryFailsWhenServerFails(t *testing.T) {
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 0))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	fake.setReply(rawReply(http.StatusServiceUnavailable, "no healthy upstream"))

	token, err := c.AccessToken()(context.Background(), false)
	if err == nil {
		t.Fatalf("expected an error, got token %q", token)
	}
	for _, want := range []string{"failed renewing login5 access token", "login5 HTTP 503"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestAccessTokenAfterExpiryRenews(t *testing.T) {
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 0))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	fake.setReply(okReply("alice", "token-b", 3600))

	token, err := c.AccessToken()(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "token-b" {
		t.Errorf("got token %q, want the renewed token-b", token)
	}

	// and it stays on the fast path afterwards
	token, err = c.AccessToken()(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "token-b" {
		t.Errorf("got token %q, want token-b", token)
	}
	if n := fake.requestCount(); n != 2 {
		t.Errorf("server saw %d requests, want 2 (login + one renewal)", n)
	}
}

func TestAccessTokenConcurrentCallsRenewOnce(t *testing.T) {
	c, fake := newTestLogin5(t, okReply("alice", "token-a", 600))
	if err := c.Login(context.Background(), storedCredential()); err != nil {
		t.Fatal(err)
	}

	// make the renewal slow enough that every goroutine queues behind it
	fake.setReply(func(req *pb.LoginRequest) (int, []byte) {
		time.Sleep(50 * time.Millisecond)
		return okReply("alice", "token-b", 3600)(req)
	})

	const callers = 20
	var wg sync.WaitGroup
	tokens := make([]string, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tokens[i], errs[i] = c.AccessToken()(context.Background(), false)
		}(i)
	}
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Errorf("caller %d: unexpected error: %v", i, errs[i])
		}
		if tokens[i] != "token-b" {
			t.Errorf("caller %d: got token %q, want token-b", i, tokens[i])
		}
	}
	if n := fake.requestCount(); n != 2 {
		t.Errorf("server saw %d requests, want 2 (login + exactly one renewal)", n)
	}
}

func TestPrintableSnippet(t *testing.T) {
	long := strings.Repeat("x", 200)
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"no healthy upstream\n", "no healthy upstream"},
		{"\x00\x01ab\xff\ncd", "abcd"},
		{long, long[:errorBodySnippetLen]},
		{"", "(0 bytes, none printable)"},
		{"\x00\x00", "(2 bytes, none printable)"},
	} {
		if got := printableSnippet([]byte(tc.in), errorBodySnippetLen); got != tc.want {
			t.Errorf("printableSnippet(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
