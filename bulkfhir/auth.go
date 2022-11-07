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
	token  string
	expiry time.Time
}

func (bt *bearerToken) shouldRenew(alwaysAuthenticateIfNoExpiresIn bool) bool {
	if bt == nil || bt.token == "" {
		return true
	}
	if bt.expiry.IsZero() {
		if alwaysAuthenticateIfNoExpiresIn {
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

// HTTPBasicOAuthAuthenticator is an implementation of Authenticator which performs a
// 2-legged OAuth2 handshake using HTTP Basic Authentication to obtain an access token,
// which is presented as an "Authorization: Bearer {token}" header in all requests.
//
// Note: this implementation is not thread safe.
type HTTPBasicOAuthAuthenticator struct {
	username, password string
	tokenURL           string
	scopes             []string

	token *bearerToken

	alwaysAuthenticateIfNoExpiresIn bool
	defaultExpiry                   time.Duration
}

// HTTPBasicOAuthOptions contains optional parameters used when creating a
// HTTPBasicOAuthAuthenticator.
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

// NewHTTPBasicOAuthAuthenticator creates a new HTTPBasicOAuthAuthenticator, validating
// its parameters. The username and password are typically a client ID and client secret
// (respectively) supplied by the Bulk FHIR Provider.
func NewHTTPBasicOAuthAuthenticator(username, password, tokenURL string, opts *HTTPBasicOAuthOptions) (*HTTPBasicOAuthAuthenticator, error) {
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

	a := &HTTPBasicOAuthAuthenticator{
		username: username,
		password: password,
		tokenURL: tokenURL,
	}
	if opts != nil {
		a.scopes = opts.Scopes
		a.alwaysAuthenticateIfNoExpiresIn = opts.AlwaysAuthenticateIfNoExpiresIn
		a.defaultExpiry = opts.DefaultExpiry
	}

	return a, nil
}

// buildBody serializes the provided slice of scopes for use in
// authenticate's HTTP body using the expected urlencoded scheme, and adds in
// the default grant_type.
func (hboa *HTTPBasicOAuthAuthenticator) buildBody() io.Reader {
	if len(hboa.scopes) == 0 {
		return nil
	}

	v := url.Values{}
	v.Add("scope", strings.Join(hboa.scopes, " "))
	v.Add("grant_type", "client_credentials")

	return bytes.NewBufferString(v.Encode())
}

// Authenticate is Authenticator.Authenticate.
//
// This Authenticator performs 2-legged OAuth using HTTP Basic Authentication to
// obtain an expiry token.
func (hboa *HTTPBasicOAuthAuthenticator) Authenticate(hc *http.Client) error {
	req, err := http.NewRequest(http.MethodPost, hboa.tokenURL, hboa.buildBody())
	if err != nil {
		return err
	}

	req.SetBasicAuth(hboa.username, hboa.password)
	req.Header.Add(acceptHeader, acceptHeaderJSON)
	req.Header.Add(contentTypeHeader, contentTypeFormURLEncoded)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unexpected status code %v, but also had an error parsing error body: %v %w", resp.StatusCode, err, ErrorUnexpectedStatusCode)
		}
		return fmt.Errorf("unexpected status code %v with error body: %s %w", resp.StatusCode, respBody, ErrorUnexpectedStatusCode)
	}

	var tr tokenResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&tr); err != nil {
		return err
	}

	hboa.token = tr.toBearerToken(hboa.defaultExpiry)

	return nil
}

// tokenResponse represents an OAuth response from a token endpoint.
type tokenResponse struct {
	Token         string `json:"access_token"`
	ExpiresInSecs int64  `json:"expires_in"`
}

func (tr *tokenResponse) toBearerToken(defaultExpiry time.Duration) *bearerToken {
	bt := &bearerToken{token: tr.Token}
	if tr.ExpiresInSecs > 0 {
		bt.expiry = timeNow().Add(time.Duration(tr.ExpiresInSecs) * time.Second)
	} else if defaultExpiry > 0 {
		bt.expiry = timeNow().Add(defaultExpiry)
	}
	return bt
}

// AuthenticateIfNecessary is Authenticator.AuthenticateIfNecessary.
//
// This Authenticator performs 2-legged OAuth using HTTP Basic Authentication to
// obtain an expiry token.
//
// Authentication is necessary if:
//   - Credential exchange has never been performed (i.e. no token is set)
//   - The obtained token has expired, based on either an "expires_in" value
//     from a previous request, or a default expiry set when the authenticator
//     was created.
//   - No expiry time is available, and AlwaysAuthenticateIfNoExpiresIn was set
//     to true then the authenticator was created.
func (hboa *HTTPBasicOAuthAuthenticator) AuthenticateIfNecessary(hc *http.Client) error {
	if hboa.token.shouldRenew(hboa.alwaysAuthenticateIfNoExpiresIn) {
		return hboa.Authenticate(hc)
	}
	return nil
}

// AddAuthenticationToRequest is Authenticator.AddAuthenticationToRequest.
//
// This Authenticator adds an access token as an Authorization: Bearer {token}
// header, automatically requesting/refreshing the token as necessary.
func (hboa *HTTPBasicOAuthAuthenticator) AddAuthenticationToRequest(hc *http.Client, req *http.Request) error {
	if err := hboa.AuthenticateIfNecessary(hc); err != nil {
		return err
	}
	hboa.token.addHeader(req)
	return nil
}
