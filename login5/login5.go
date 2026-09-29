package login5

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	pb "github.com/devgianlu/go-librespot/proto/spotify/login5/v3"
	credentialspb "github.com/devgianlu/go-librespot/proto/spotify/login5/v3/credentials"
	"google.golang.org/protobuf/proto"
)

const (
	// accessTokenRefreshBefore is how long before the access token expires that
	// AccessToken starts renewing it. A renewal that fails inside this window is
	// logged and the still-valid token is returned, so a transient login5 outage
	// right at expiry time does not take the session down.
	accessTokenRefreshBefore = 15 * time.Minute

	// errorBodySnippetLen is how many printable bytes of a non-200 login5
	// response body are quoted in the error message.
	errorBodySnippetLen = 120
)

type LoginError struct {
	Code pb.LoginError
}

func (e *LoginError) Error() string {
	return fmt.Sprintf("failed authenticating with login5: %v", e.Code)
}

type Login5 struct {
	log     librespot.Logger
	baseUrl *url.URL
	client  *http.Client

	deviceId    string
	clientToken string

	loginOk     *pb.LoginOk
	loginOkExp  time.Time
	loginOkLock sync.RWMutex

	// renewLock serialises access token renewals, so that concurrent
	// AccessToken calls inside the refresh window do not all hit login5.
	renewLock sync.Mutex
}

func NewLogin5(log librespot.Logger, client *http.Client, deviceId, clientToken string) *Login5 {
	return newLogin5WithBaseURL(log, client, "https://login5.spotify.com/", deviceId, clientToken)
}

// newLogin5WithBaseURL is NewLogin5 with an explicit base URL, so that tests
// can point the client at a fake login5 server.
func newLogin5WithBaseURL(log librespot.Logger, client *http.Client, rawBaseUrl, deviceId, clientToken string) *Login5 {
	baseUrl, err := url.Parse(rawBaseUrl)
	if err != nil {
		panic("invalid login5 base URL")
	}

	// JoinPath on an empty path yields a relative request URI ("v3/login"),
	// which servers reject
	if baseUrl.Path == "" {
		baseUrl.Path = "/"
	}

	return &Login5{
		log:         log,
		baseUrl:     baseUrl,
		client:      client,
		deviceId:    deviceId,
		clientToken: clientToken,
	}
}

// printableSnippet returns up to limit printable ASCII bytes of body, so that
// a text or HTML error page reads cleanly in a log line and a binary body
// cannot corrupt it.
func printableSnippet(body []byte, limit int) string {
	out := make([]byte, 0, limit)
	for _, b := range body {
		if b < ' ' || b > '~' {
			continue
		}

		out = append(out, b)
		if len(out) == limit {
			break
		}
	}

	if len(out) == 0 {
		return fmt.Sprintf("(%d bytes, none printable)", len(body))
	}

	return string(out)
}

func (c *Login5) request(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed marshalling LoginRequest: %w", err)
	}

	httpReq := &http.Request{
		Method: "POST",
		URL:    c.baseUrl.JoinPath("/v3/login"),
		Header: http.Header{
			"Accept":       []string{"application/x-protobuf"},
			"User-Agent":   []string{librespot.UserAgent()},
			"Client-Token": []string{c.clientToken},
		},
		Body: io.NopCloser(bytes.NewReader(body)),
	}

	resp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed requesting login5: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading login5 response: %w", err)
	}

	// errors from the edge (503 "no healthy upstream", HTML error pages) come
	// back as text, not protobuf: report them as such instead of as a
	// wire-format parse error
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("login5 HTTP %d: %s", resp.StatusCode, printableSnippet(respBody, errorBodySnippetLen))
	}

	var protoResp pb.LoginResponse
	if err := proto.Unmarshal(respBody, &protoResp); err != nil {
		return nil, fmt.Errorf("failed unmarshalling LoginResponse (HTTP %d, %d bytes): %w", resp.StatusCode, len(respBody), err)
	}

	// a body that parses but carries none of ok, error or challenges is not an
	// answer, it is an empty 200 from a broken backend
	if protoResp.GetResponse() == nil {
		return nil, fmt.Errorf("login5 returned an empty response (HTTP %d, %d bytes)", resp.StatusCode, len(respBody))
	}

	return &protoResp, nil
}

func (c *Login5) Login(ctx context.Context, credentials proto.Message) error {
	c.loginOkLock.Lock()
	defer c.loginOkLock.Unlock()

	req := &pb.LoginRequest{
		ClientInfo: &pb.ClientInfo{
			ClientId: librespot.ClientIdHex,
			DeviceId: c.deviceId,
		},
	}

	switch lm := credentials.(type) {
	case *credentialspb.StoredCredential:
		req.LoginMethod = &pb.LoginRequest_StoredCredential{StoredCredential: lm}
	case *credentialspb.FacebookAccessToken:
		req.LoginMethod = &pb.LoginRequest_FacebookAccessToken{FacebookAccessToken: lm}
	case *credentialspb.OneTimeToken:
		req.LoginMethod = &pb.LoginRequest_OneTimeToken{OneTimeToken: lm}
	case *credentialspb.ParentChildCredential:
		req.LoginMethod = &pb.LoginRequest_ParentChildCredential{ParentChildCredential: lm}
	case *credentialspb.AppleSignInCredential:
		req.LoginMethod = &pb.LoginRequest_AppleSignInCredential{AppleSignInCredential: lm}
	case *credentialspb.SamsungSignInCredential:
		req.LoginMethod = &pb.LoginRequest_SamsungSignInCredential{SamsungSignInCredential: lm}
	case *credentialspb.GoogleSignInCredential:
		req.LoginMethod = &pb.LoginRequest_GoogleSignInCredential{GoogleSignInCredential: lm}
	default:
		return fmt.Errorf("invalid credentials: %v", lm)
	}

	resp, err := c.request(ctx, req)
	if err != nil {
		return fmt.Errorf("failed requesting login5 endpoint: %w", err)
	}

	if ch := resp.GetChallenges(); ch != nil && len(ch.Challenges) > 0 {
		req.LoginContext = resp.LoginContext
		req.ChallengeSolutions = &pb.ChallengeSolutions{}

		// solve challenges
		for _, c := range ch.Challenges {
			switch cc := c.Challenge.(type) {
			case *pb.Challenge_Hashcash:
				sol := solveHashcash(req.LoginContext, cc.Hashcash)
				req.ChallengeSolutions.Solutions = append(req.ChallengeSolutions.Solutions, &pb.ChallengeSolution{
					Solution: &pb.ChallengeSolution_Hashcash{Hashcash: sol},
				})
			case *pb.Challenge_Code:
				return fmt.Errorf("login5 code challenge not supported")
			}
		}

		resp, err = c.request(ctx, req)
		if err != nil {
			return fmt.Errorf("failed requesting login5 endpoint with challenge solutions: %w", err)
		}
	}

	if ok := resp.GetOk(); ok != nil {
		c.loginOk = ok
		c.loginOkExp = time.Now().Add(time.Duration(c.loginOk.AccessTokenExpiresIn) * time.Second)
		c.log.WithField("username", librespot.ObfuscateUsername(c.loginOk.Username)).
			Infof("authenticated Login5")
		return nil
	} else {
		return &LoginError{Code: resp.GetError()}
	}
}

func (c *Login5) Username() string {
	c.loginOkLock.RLock()
	defer c.loginOkLock.RUnlock()

	if c.loginOk == nil {
		panic("login5 not authenticated")
	}

	return c.loginOk.Username
}

func (c *Login5) StoredCredential() []byte {
	c.loginOkLock.RLock()
	defer c.loginOkLock.RUnlock()

	if c.loginOk == nil {
		panic("login5 not authenticated")
	}

	return c.loginOk.StoredCredential
}

// currentToken returns the cached access token and the time it expires at.
func (c *Login5) currentToken() (string, time.Time) {
	c.loginOkLock.RLock()
	defer c.loginOkLock.RUnlock()

	if c.loginOk == nil {
		panic("login5 not authenticated")
	}

	return c.loginOk.AccessToken, c.loginOkExp
}

// renew logs in again with the stored credential, replacing the cached token.
// It must not hold loginOkLock, since Login takes it for writing.
func (c *Login5) renew(ctx context.Context) error {
	c.loginOkLock.RLock()
	username, storedCred := c.loginOk.Username, c.loginOk.StoredCredential
	c.loginOkLock.RUnlock()

	c.log.Debug("renewing login5 access token")
	return c.Login(ctx, &credentialspb.StoredCredential{
		Username: username,
		Data:     storedCred,
	})
}

func (c *Login5) AccessToken() librespot.GetLogin5TokenFunc {
	return func(ctx context.Context, force bool) (string, error) {
		token, exp := c.currentToken()

		// if not asked to force a new token and not close to expiry, just return it
		if !force && time.Now().Before(exp.Add(-accessTokenRefreshBefore)) {
			return token, nil
		}

		// one renewal at a time: a caller that queued behind another renewal
		// re-checks and normally finds the token it was waiting for
		c.renewLock.Lock()
		defer c.renewLock.Unlock()

		token, exp = c.currentToken()
		if !force && time.Now().Before(exp.Add(-accessTokenRefreshBefore)) {
			return token, nil
		}

		err := c.renew(ctx)
		if err == nil {
			token, _ = c.currentToken()
			return token, nil
		}

		// inside the refresh window the old token is still valid: keep using it
		// and try again on the next call instead of failing the caller
		if remaining := time.Until(exp); !force && remaining > 0 {
			c.log.WithError(err).Warnf("failed renewing login5 access token early, keeping the current one for %s", remaining.Round(time.Second))
			return token, nil
		}

		return "", fmt.Errorf("failed renewing login5 access token: %w", err)
	}
}
