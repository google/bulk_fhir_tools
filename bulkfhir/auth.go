// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bulkfhir

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/golang-jwt/jwt"
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

// BearerToken encapsulates a bearer token presented as an Authorization header.
type BearerToken struct {
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
func (bt *BearerToken) shouldRenew() bool {
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

func (bt *BearerToken) addHeader(req *http.Request) {
	req.Header.Set(authorizationHeader, fmt.Sprintf("Bearer %s", bt.token))
}

// CredentialExchanger is used by bearerTokenAuthenticator to exchange
// long-lived credentials for a short lived bearer token.
type CredentialExchanger interface {
	Authenticate(hc *http.Client) (*BearerToken, error)
}

// BearerTokenAuthenticator is an implementation of Authenticator which uses a
// CredentialExchanger to obtain a bearer token which is presented in an
// Authorization header.
//
// Note: this implementation is not thread safe.
type BearerTokenAuthenticator struct {
	Exchanger CredentialExchanger
	token     *BearerToken
}

// Authenticate is Authenticator.Authenticate.
//
// This Authenticator uses the CredentialExchanger it contains to obtain a
// bearer token.
func (bta *BearerTokenAuthenticator) Authenticate(hc *http.Client) error {
	token, err := bta.Exchanger.Authenticate(hc)
	if err != nil {
		return err
	}
	bta.token = token
	return nil
}

// AuthenticateIfNecessary is Authenticator.AuthenticateIfNecessary.
//
// This Authenticator uses the CredentialExchanger it contains to obtain a
// bearer token.
func (bta *BearerTokenAuthenticator) AuthenticateIfNecessary(hc *http.Client) error {
	if bta.token.shouldRenew() {
		return bta.Authenticate(hc)
	}
	return nil
}

// AddAuthenticationToRequest is Authenticator.AddAuthenticationToRequest.
//
// This Authenticator adds an access token as an Authorization: Bearer {token}
// header, automatically requesting/refreshing the token as necessary.
func (bta *BearerTokenAuthenticator) AddAuthenticationToRequest(hc *http.Client, req *http.Request) error {
	if err := bta.AuthenticateIfNecessary(hc); err != nil {
		return err
	}
	bta.token.addHeader(req)
	return nil
}

// tokenResponse represents an OAuth response from a token endpoint.
type tokenResponse struct {
	Token         string
	ExpiresInSecs int64
}

// UnmarshalJSON contains custom logic needed to unmarshal a json tokenResponse.
// This is needed because some servers (like BCDA) incorrectly return expires_in
// as a string whereas most servers return a numeric type which is in accordance
// with the RFC6749 specification. It is called automatically when using
// encoding/json to unmarshal the data, since this allows tokenResponse to meet
// the encoding/json.Unmarshaler interface.
func (tr *tokenResponse) UnmarshalJSON(data []byte) error {
	t := struct {
		Token         string `json:"access_token"`
		ExpiresInSecs any    `json:"expires_in"`
	}{}
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	tr.Token = t.Token

	switch v := t.ExpiresInSecs.(type) {
	case float64:
		// This is what we expect. It's float64 because that's how the unmarshaler
		// unmarshals generic numeric types.
		if math.Mod(v, 1.0) != 0 {
			// This is actually a non-int for some reason. This may be okay, but for
			// now we treat it as an error to keep things simple.
			return fmt.Errorf("expires_in was a non-integer number: %v", v)
		}
		tr.ExpiresInSecs = int64(v)
	case string:
		// We got a string value. While this is not in accordance with the OAuth
		// spec, BCDA is known to return a string value for now so we support it.
		// See b/258310551 for more.
		expiresInInt, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse string expires_in: %w", err)
		}
		tr.ExpiresInSecs = expiresInInt
	case nil:
		return nil
	default:
		return fmt.Errorf("unexpected data type for expires_in: %T", t.ExpiresInSecs)
	}

	return nil
}

func (tr *tokenResponse) toBearerToken(defaultExpiry time.Duration, alwaysAuthenticateIfNoExpiry bool) *BearerToken {
	bt := &BearerToken{
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

// DoOAuthExchange sends a HTTP request which is expected to return a JSON
// response with "token" and "expires_in" fields.
func DoOAuthExchange(hc *http.Client, req *http.Request, defaultExpiry time.Duration, alwaysAuthenticateIfNoExpiresIn bool) (*BearerToken, error) {
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

	return tr.toBearerToken(defaultExpiry, alwaysAuthenticateIfNoExpiresIn), nil
}

// httpBasicOAuthExchanger is an implementation of CredentialExchanger for use
// with bearerTokenAuthenticator which performs a 2-legged OAuth2 handshake
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

// Authenticate is CredentialExchanger.Authenticate.
//
// This CredentialExchanger performs 2-legged OAuth using HTTP Basic
// Authentication to obtain an expiry token.
func (hboe *httpBasicOAuthExchanger) Authenticate(hc *http.Client) (*BearerToken, error) {
	req, err := http.NewRequest(http.MethodPost, hboe.tokenURL, hboe.buildBody())
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(hboe.username, hboe.password)
	req.Header.Add(acceptHeader, acceptHeaderJSON)
	req.Header.Add(contentTypeHeader, contentTypeFormURLEncoded)

	return DoOAuthExchange(hc, req, hboe.defaultExpiry, hboe.alwaysAuthenticateIfNoExpiresIn)
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

	return &BearerTokenAuthenticator{Exchanger: e}, nil
}

// A JWTKeyProvider provides the RSA private key used for signing JSON Web Tokens.
type JWTKeyProvider interface {
	Key() (*rsa.PrivateKey, error)
	KeyID() string
}

// pemFileKeyProvider is an implementation of JWTKeyProvider which reads a
// PEM-encoded key from a local file.
type pemFileKeyProvider struct {
	filename, keyID string
	key             *rsa.PrivateKey
}

func (pfkp *pemFileKeyProvider) Key() (*rsa.PrivateKey, error) {
	if pfkp.key != nil {
		return pfkp.key, nil
	}
	f, err := os.Open(pfkp.filename)
	if err != nil {
		return nil, err
	}
	keyBytes, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	pfkp.key, err = jwt.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		return nil, err
	}
	return pfkp.key, nil
}

func (pfkp *pemFileKeyProvider) KeyID() string {
	return pfkp.keyID
}

// NewPEMFileKeyProvider returns a JWTKeyProvider which reads a PEM-encoded key
// from the given file.
func NewPEMFileKeyProvider(filename, keyID string) JWTKeyProvider {
	return &pemFileKeyProvider{filename: filename, keyID: keyID}
}

type jwtOAuthExchanger struct {
	issuer, subject, tokenURL       string
	keyProvider                     JWTKeyProvider
	jwtLifetime                     time.Duration
	scopes                          []string
	defaultExpiry                   time.Duration
	alwaysAuthenticateIfNoExpiresIn bool
}

// buildBody serializes the provided slice of scopes for use in
// authenticate's HTTP body using the expected urlencoded scheme, and adds in
// the default grant_type.
func (joe *jwtOAuthExchanger) buildBody() (io.Reader, error) {
	key, err := joe.keyProvider.Key()
	if err != nil {
		return nil, err
	}
	now := timeNow()
	token := jwt.NewWithClaims(jwt.SigningMethodRS384, jwt.StandardClaims{
		ExpiresAt: now.Add(joe.jwtLifetime).Unix(),
		Issuer:    joe.issuer,
		Subject:   joe.subject,
		Audience:  joe.tokenURL,
		Id:        uuid.New().String(),
	})
	token.Header["kid"] = joe.keyProvider.KeyID()
	tokenString, err := token.SignedString(key)
	if err != nil {
		return nil, err
	}

	v := url.Values{
		"grant_type":            []string{"client_credentials"},
		"client_assertion":      []string{tokenString},
		"client_assertion_type": []string{"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
	}
	if len(joe.scopes) > 0 {
		v.Add("scope", strings.Join(joe.scopes, " "))
	}

	return bytes.NewBufferString(v.Encode()), nil
}

// Authenticate is CredentialExchanger.Authenticate.
//
// This CredentialExchanger performs 2-legged OAuth using HTTP Basic
// Authentication to obtain an expiry token.
func (joe *jwtOAuthExchanger) Authenticate(hc *http.Client) (*BearerToken, error) {
	body, err := joe.buildBody()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, joe.tokenURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add(acceptHeader, acceptHeaderJSON)
	req.Header.Add(contentTypeHeader, contentTypeFormURLEncoded)

	return DoOAuthExchange(hc, req, joe.defaultExpiry, joe.alwaysAuthenticateIfNoExpiresIn)
}

// JWTOAuthOptions contains optional parameters used by NewJWTOAuthAuthenticator.
type JWTOAuthOptions struct {
	// How long the generated JWT is valid for (according to its "exp" claim).
	// Defaults to 5 minutes if unset.
	JWTLifetime time.Duration

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

// NewJWTOAuthAuthenticator creates a new Authenticator which uses  2-legged
// OAuth with JWT authentication (according to RFC9068) to obtain a bearer token.
func NewJWTOAuthAuthenticator(issuer, subject, tokenURL string, keyProvider JWTKeyProvider, opts *JWTOAuthOptions) (Authenticator, error) {
	if issuer == "" || subject == "" {
		return nil, errors.New("issuer and subject must be specified for JWT OAuth authentication")
	}
	parsed, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token URL %q: %w", tokenURL, err)
	}
	if !parsed.IsAbs() {
		return nil, fmt.Errorf("token URL %q is not absolute", tokenURL)
	}

	e := &jwtOAuthExchanger{
		issuer:      issuer,
		subject:     subject,
		tokenURL:    tokenURL,
		keyProvider: keyProvider,
		jwtLifetime: time.Minute,
	}
	if opts != nil {
		e.scopes = opts.Scopes
		e.alwaysAuthenticateIfNoExpiresIn = opts.AlwaysAuthenticateIfNoExpiresIn
		e.defaultExpiry = opts.DefaultExpiry
		if opts.JWTLifetime > 0 {
			e.jwtLifetime = opts.JWTLifetime
		}
	}

	return &BearerTokenAuthenticator{Exchanger: e}, nil
}
