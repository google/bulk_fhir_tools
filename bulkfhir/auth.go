package bulkfhir

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Used for testing.
var timeNow = time.Now

const authorizationHeader = "Authorization"

// Authenticator defines a module used for obtaining authentication credentials
// and attaching them to outbound requests to the Bulk FHIR APIs.
type Authenticator interface {

	// Authenticate unconditionally performs any credential exchange required to
	// make requests. It is generally not necessary to call this method, as it
	// will be called automatically by AddAuthenticationToRequest if credentials
	// have not yet been exchanged or have expired.
	Authenticate(hc *http.Client) error

	// AuthenticateIfNecessary performs any credential exchange required to make
	// requests, if the credentials have expired or have not yet been exchanged.
	// This can be used if you need to track authentication errors, but does not
	// need to be called otherwise; authentication will be done automatically when
	// requests are made using AddAuthenticationToRequest.
	AuthenticateIfNecessary(hc *http.Client) error

	// Add authentication credentials to an outbound request. This may perform
	// additional requests to perform credential exchange if required by the
	// authentication mechanism, both before any initial request, and on
	// subsequent requests if any acquired credentials have expired.
	//
	// Implementations should call their own AuthenticateIfNecessary method if
	// credential exchange is necessary.
	AddAuthenticationToRequest(hc *http.Client, req *http.Request) error
}

// bearerToken encapsulates a bearer token presented as an Authorization header.
type bearerToken struct {
	token                        string
	expiry                       time.Time
	alwaysAuthenticateIfNoExpiry bool
}

// shouldRenew returns whether this token needs to be renewed.
//
// Renewal is necessary if:
//   - Credential exchange has never been performed (i.e. no token is set)
//   - The obtained token has expired, based on either an "expires_in" value
//     from a previous request, or a default expiry set when the authenticator
//     was created.
//   - No expiry time is available, and alwaysAuthenticateIfNoExpiry is true.
func (bt *bearerToken) shouldRenew() bool {
	if bt == nil || bt.token == "" {
		return true
	}
	if bt.expiry.IsZero() {
		if bt.alwaysAuthenticateIfNoExpiry {
			return true
		}
	} else if bt.expiry.Before(timeNow()) {
		return true
	}
	return false
}

func (bt *bearerToken) addHeader(req *http.Request) {
	req.Header.Set(authorizationHeader, fmt.Sprintf("Bearer %s", bt.token))
}

// credentialExchanger is used by BearerTokenAuthenticator to exchange
// long-lived credentials for a short lived bearer token.
type credentialExchanger interface {
	authenticate(hc *http.Client) (*bearerToken, error)
}

// bearerTokenAuthenticator is an implementation of Authenticator which uses a
// credentialExchanger to obtain a bearer token which is presented in an
// Authorization header.
//
// Note: this implementation is not thread safe.
type bearerTokenAuthenticator struct {
	exchanger credentialExchanger
	token     *bearerToken
}

// Authenticate is Authenticator.Authenticate.
//
// This Authenticator uses the credentialExchanger it contains to obtain a
// bearer token.
func (bta *bearerTokenAuthenticator) Authenticate(hc *http.Client) error {
	token, err := bta.exchanger.authenticate(hc)
	if err != nil {
		return err
	}
	bta.token = token
	return nil
}

// AuthenticateIfNecessary is Authenticator.AuthenticateIfNecessary.
//
// This Authenticator uses the credentialExchanger it contains to obtain a
// bearer token.
func (bta *bearerTokenAuthenticator) AuthenticateIfNecessary(hc *http.Client) error {
	if bta.token.shouldRenew() {
		return bta.Authenticate(hc)
	}
	return nil
}

// AddAuthenticationToRequest is Authenticator.AddAuthenticationToRequest.
//
// This Authenticator adds an access token as an Authorization: Bearer {token}
// header, automatically requesting/refreshing the token as necessary.
func (bta *bearerTokenAuthenticator) AddAuthenticationToRequest(hc *http.Client, req *http.Request) error {
	if err := bta.AuthenticateIfNecessary(hc); err != nil {
		return err
	}
	bta.token.addHeader(req)
	return nil
}

// tokenResponse represents an OAuth response from a token endpoint.
type tokenResponse struct {
	Token         string `json:"access_token"`
	ExpiresInSecs int64  `json:"expires_in"`
}

func (tr *tokenResponse) toBearerToken(defaultExpiry time.Duration, alwaysAuthenticateIfNoExpiry bool) *bearerToken {
	bt := &bearerToken{
		token:                        tr.Token,
		alwaysAuthenticateIfNoExpiry: alwaysAuthenticateIfNoExpiry,
	}
	if tr.ExpiresInSecs > 0 {
		bt.expiry = timeNow().Add(time.Duration(tr.ExpiresInSecs) * time.Second)
	} else if defaultExpiry > 0 {
		bt.expiry = timeNow().Add(defaultExpiry)
	}
	return bt
}

// httpBasicOAuthExchanger is an implementation of credentialExchanger for use
// with BearerTokenAuthenticator which performs a 2-legged OAuth2 handshake
// using HTTP Basic Authentication to obtain an access token, which is presented
// as an "Authorization: Bearer {token}" header in all requests.
//
// Note: this implementation is not thread safe.
type httpBasicOAuthExchanger struct {
	username, password, tokenURL    string
	scopes                          []string
	defaultExpiry                   time.Duration
	alwaysAuthenticateIfNoExpiresIn bool
}

// buildBody serializes the provided slice of scopes for use in
// authenticate's HTTP body using the expected urlencoded scheme, and adds in
// the default grant_type.
func (hboe *httpBasicOAuthExchanger) buildBody() io.Reader {
	if len(hboe.scopes) == 0 {
		return nil
	}

	v := url.Values{}
	v.Add("scope", strings.Join(hboe.scopes, " "))
	v.Add("grant_type", "client_credentials")

	return bytes.NewBufferString(v.Encode())
}

// authenticate is credentialExchanger.authenticate.
//
// This credentialExchanger performs 2-legged OAuth using HTTP Basic
// Authentication to obtain an expiry token.
func (hboe *httpBasicOAuthExchanger) authenticate(hc *http.Client) (*bearerToken, error) {
	req, err := http.NewRequest(http.MethodPost, hboe.tokenURL, hboe.buildBody())
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(hboe.username, hboe.password)
	req.Header.Add(acceptHeader, acceptHeaderJSON)
	req.Header.Add(contentTypeHeader, contentTypeFormURLEncoded)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("unexpected status code %v, but also had an error parsing error body: %v %w", resp.StatusCode, err, ErrorUnexpectedStatusCode)
		}
		return nil, fmt.Errorf("unexpected status code %v with error body: %s %w", resp.StatusCode, respBody, ErrorUnexpectedStatusCode)
	}

	var tr tokenResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&tr); err != nil {
		return nil, err
	}

	return tr.toBearerToken(hboe.defaultExpiry, hboe.alwaysAuthenticateIfNoExpiresIn), nil
}

// HTTPBasicOAuthOptions contains optional parameters used by
// NewHTTPBasicOAuthAuthenticator.
type HTTPBasicOAuthOptions struct {
	// OAuth scopes used when authenticating.
	Scopes []string

	// Whether the authenticator should always refresh if the authentication
	// server does not provide an "expires_in" duration in the response. The
	// default behaviour is to automatically authenticate upon first use (when
	// AuthenticateIfNecessary or AddAuthenticationToRequest is called), and then
	// to not authenticate again if no expiry time can be determined.
	//
	// Consider using DefaultExpiry instead to provide an expiry duration that is
	// used for determining the expiry time after each credential exchange.
	AlwaysAuthenticateIfNoExpiresIn bool

	// A default expiry duration to use if the authentication server does not
	// provide an "expires_in" duration in the response.
	DefaultExpiry time.Duration
}

// NewHTTPBasicOAuthAuthenticator creates a new Authenticator which uses
// 2-legged OAuth with HTTP Basic authentication to obtain a bearer token.
// The username and password are typically a client ID and client secret
// (respectively) supplied by the Bulk FHIR Provider.
func NewHTTPBasicOAuthAuthenticator(username, password, tokenURL string, opts *HTTPBasicOAuthOptions) (Authenticator, error) {
	if username == "" || password == "" {
		return nil, errors.New("username and password must be specified for HTTP Basic OAuth authentication")
	}
	parsed, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token URL %q: %w", tokenURL, err)
	}
	if !parsed.IsAbs() {
		return nil, fmt.Errorf("token URL %q is not absolute", tokenURL)
	}

	e := &httpBasicOAuthExchanger{
		username: username,
		password: password,
		tokenURL: tokenURL,
	}
	if opts != nil {
		e.scopes = opts.Scopes
		e.alwaysAuthenticateIfNoExpiresIn = opts.AlwaysAuthenticateIfNoExpiresIn
		e.defaultExpiry = opts.DefaultExpiry
	}

	return &bearerTokenAuthenticator{exchanger: e}, nil
}
