package bulkfhir

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/golang-jwt/jwt"
	"bitbucket.org/creachadair/stringset"
)

func TestHTTPBasicOAuthAuthenticator_AddAuthenticationToRequest(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"

	expectedPath := "/auth/token"
	expectedAcceptValue := "application/json"
	expectedContentTypeValue := "application/x-www-form-urlencoded"
	expectedGrantTypeValue := "client_credentials"

	expectedHeader := "Bearer 123"

	cases := []struct {
		name   string
		scopes []string
	}{
		{
			name:   "NoScopes",
			scopes: []string{},
		},
		{
			name:   "OneScope",
			scopes: []string{"a"},
		},
		{
			name:   "MultiScopes",
			scopes: []string{"a", "b", "c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.String() != expectedPath {
					t.Errorf("Authenticate(%s, %s) made request with unexpected path. got: %v, want: %v", clientID, clientSecret, req.URL.String(), expectedPath)
				}

				id, sec, ok := req.BasicAuth()
				if !ok {
					t.Errorf("Authenticate(%s, %s) basic auth not OK.", clientID, clientSecret)
				}
				if id != clientID {
					t.Errorf("Authenticate(%s, %s) sent unexpected clientID, got %s, want: %s", clientID, clientSecret, id, clientID)
				}
				if sec != clientSecret {
					t.Errorf("Authenticate(%s, %s) sent unexpected clientSecret, got %s, want: %s", clientID, clientSecret, sec, clientSecret)
				}

				if len(tc.scopes) > 0 {
					err := req.ParseForm()
					if err != nil {
						t.Errorf("Authenticate(%s, %s) sent a body that could not be parsed as a form: %s", clientID, clientSecret, err)
					}

					if got := len(req.Form["scope"]); got != 1 {
						t.Errorf("Authenticate(%s, %s) sent invalid number of scope values. got: %v, want: %v", clientID, clientSecret, got, 1)
					}
					splitScopes := strings.Split(req.Form["scope"][0], " ")
					if diff := cmp.Diff(splitScopes, tc.scopes, cmpopts.SortSlices(func(a, b string) bool { return a > b })); diff != "" {
						t.Errorf("Authenticate(%s, %s) sent invalid scopes. diff: %s", clientID, clientSecret, diff)
					}
					if got := len(req.Form["grant_type"]); got != 1 {
						t.Errorf("Authenticate(%s, %s) sent invalid number of grant_type values. got: %v, want: %v", clientID, clientSecret, got, 1)
					}
					if got := req.Form["grant_type"][0]; got != expectedGrantTypeValue {
						t.Errorf("Authenticate(%s, %s) sent invalid grant_type value. got: %v, want: %v", clientID, clientSecret, got, expectedGrantTypeValue)
					}

					contentTypeVals, ok := req.Header["Content-Type"]
					if !ok {
						t.Errorf("Authenticate(%s, %s) did not send Content-Type header", clientID, clientSecret)
					}
					if len(contentTypeVals) != 1 {
						t.Errorf("Authenticate(%s, %s) sent Content-Type header with unexpected number of values. got: %v, want: %v", clientID, clientSecret, len(contentTypeVals), 1)
					}
					if got := contentTypeVals[0]; got != expectedContentTypeValue {
						t.Errorf("Authenticate(%s, %s) sent Content-Type header with unexpected value. got: %v, want: %v", clientID, clientSecret, got, expectedContentTypeValue)
					}
				}

				accValues, ok := req.Header["Accept"]
				if !ok {
					t.Errorf("Authenticate(%s, %s) did not send Accept header", clientID, clientSecret)
				}

				if len(accValues) != 1 {
					t.Errorf("Authenticate(%s, %s) sent Content-Type header with unexpected number of values. got: %v, want: %v", clientID, clientSecret, len(accValues), 1)
				}

				if got := accValues[0]; got != expectedAcceptValue {
					t.Errorf("Authenticate(%s, %s) did not send expected Accept header. got: %v, want: %v", clientID, clientSecret, got, expectedAcceptValue)
				}

				w.Write([]byte(`{"access_token": "123", "expires_in": 1200}`))
			}))
			defer server.Close()
			authURL := server.URL + "/auth/token"

			authenticator, err := NewHTTPBasicOAuthAuthenticator(clientID, clientSecret, authURL, &HTTPBasicOAuthOptions{Scopes: tc.scopes})
			if err != nil {
				t.Fatalf("NewHTTPBasicOAuthAuthenticator(%q, %q, %q, {Scopes: %v}) error: %v", clientID, clientSecret, authURL, tc.scopes, err)
			}

			buildRequestAndCheckHeader(t, authenticator, expectedHeader)
		})
	}
}

func TestHTTPBasicOAuthAuthenticator_Authenticate_WithError(t *testing.T) {
	wantErrBody := []byte(`an error`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(wantErrBody)
	}))
	defer server.Close()

	authURL := server.URL + "/auth/token"
	clientID := "id"
	clientSecret := "secret"
	authenticator, err := NewHTTPBasicOAuthAuthenticator(clientID, clientSecret, authURL, nil)
	if err != nil {
		t.Fatalf("NewHTTPBasicOAuthAuthenticator(%q, %q, %q, nil) error: %v", clientID, clientSecret, authURL, err)
	}
	if err := authenticator.Authenticate(http.DefaultClient); !errors.Is(err, ErrorUnexpectedStatusCode) {
		t.Errorf("Authenticate(%s, %s) returned unexpected error. got: %v, want: %v", clientID, clientSecret, err, ErrorUnexpectedStatusCode)
	}
}

func TestHTTPBasicOAuthAuthenticator_AuthenticateOnlyIfNecessary(t *testing.T) {
	for _, tc := range []struct {
		description      string
		responseTemplate string
		advanceTime      time.Duration
		opts             *HTTPBasicOAuthOptions
		wantAuthHeader   string
	}{
		{
			description:      "with expires_in, expiry not reached",
			responseTemplate: `{"access_token": "token%d", "expires_in": 1200}`,
			advanceTime:      5 * time.Minute,
			wantAuthHeader:   "Bearer token1",
		},
		{
			description:      "with expires_in, expiry reached",
			responseTemplate: `{"access_token": "token%d", "expires_in": 1200}`,
			advanceTime:      25 * time.Minute,
			wantAuthHeader:   "Bearer token2",
		},
		{
			description:      "with string expires_in, expiry reached",
			responseTemplate: `{"access_token": "token%d", "expires_in": "1200"}`,
			advanceTime:      25 * time.Minute,
			wantAuthHeader:   "Bearer token2",
		},
		{
			description:      "no expires_in, default behaviour",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      25 * time.Minute,
			wantAuthHeader:   "Bearer token1",
		},
		{
			description:      "no expires_in, always authenticate",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      0,
			opts: &HTTPBasicOAuthOptions{
				AlwaysAuthenticateIfNoExpiresIn: true,
			},
			wantAuthHeader: "Bearer token2",
		},
		{
			description:      "no expires_in, default expiry not reached",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      5 * time.Minute,
			opts: &HTTPBasicOAuthOptions{
				DefaultExpiry: 20 * time.Minute,
			},
			wantAuthHeader: "Bearer token1",
		},
		{
			description:      "no expires_in, default expiry reached",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      25 * time.Minute,
			opts: &HTTPBasicOAuthOptions{
				DefaultExpiry: 20 * time.Minute,
			},
			wantAuthHeader: "Bearer token2",
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			now := time.Now()
			timeNow = func() time.Time {
				return now
			}
			defer func() {
				timeNow = time.Now
			}()

			counter := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				counter++
				w.Write([]byte(fmt.Sprintf(tc.responseTemplate, counter)))
			}))
			defer server.Close()

			authURL := server.URL + "/auth/token"
			clientID := "id"
			clientSecret := "secret"
			authenticator, err := NewHTTPBasicOAuthAuthenticator(clientID, clientSecret, authURL, tc.opts)
			if err != nil {
				t.Fatalf("NewHTTPBasicOAuthAuthenticator(%q, %q, %q, nil) error: %v", clientID, clientSecret, authURL, err)
			}

			buildRequestAndCheckHeader(t, authenticator, "Bearer token1")

			now = now.Add(tc.advanceTime)

			buildRequestAndCheckHeader(t, authenticator, tc.wantAuthHeader)
		})
	}
}

type testKeyProvider struct {
	key   *rsa.PrivateKey
	keyID string
}

func (tkp *testKeyProvider) Key() (*rsa.PrivateKey, error) {
	return tkp.key, nil
}

func (tkp *testKeyProvider) KeyID() string {
	return tkp.keyID
}

func TestJWTOAuthAuthenticator_AddAuthenticationToRequest(t *testing.T) {
	issuer := "issuer"
	subject := "subject"

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyID := uuid.New().String()

	expectedPath := "/auth/token"
	expectedAcceptValue := "application/json"
	expectedContentTypeValue := "application/x-www-form-urlencoded"
	expectedGrantTypeValue := "client_credentials"
	expectedClientAssertionTypeValue := "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

	expectedHeader := "Bearer 123"

	seenTokenIDs := stringset.New()

	cases := []struct {
		name   string
		scopes []string
	}{
		{
			name:   "NoScopes",
			scopes: []string{},
		},
		{
			name:   "OneScope",
			scopes: []string{"a"},
		},
		{
			name:   "MultiScopes",
			scopes: []string{"a", "b", "c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			timeNow = func() time.Time {
				return now
			}
			defer func() {
				timeNow = time.Now
			}()

			authURL := ""

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.String() != expectedPath {
					t.Errorf("Authenticate() made request with unexpected path. got: %v, want: %v", req.URL.String(), expectedPath)
				}

				contentTypeVals, ok := req.Header["Content-Type"]
				if !ok {
					t.Errorf("Authenticate() did not send Content-Type header")
				} else {
					if len(contentTypeVals) != 1 {
						t.Errorf("Authenticate() sent Content-Type header with unexpected number of values. got: %v, want: %v", len(contentTypeVals), 1)
					}
					if got := contentTypeVals[0]; got != expectedContentTypeValue {
						t.Errorf("Authenticate() sent Content-Type header with unexpected value. got: %v, want: %v", got, expectedContentTypeValue)
					}
				}

				accValues, ok := req.Header["Accept"]
				if !ok {
					t.Errorf("Authenticate() did not send Accept header")
				} else {
					if len(accValues) != 1 {
						t.Errorf("Authenticate() sent Content-Type header with unexpected number of values. got: %v, want: %v", len(accValues), 1)
					}
					if got := accValues[0]; got != expectedAcceptValue {
						t.Errorf("Authenticate() did not send expected Accept header. got: %v, want: %v", got, expectedAcceptValue)
					}
				}

				if err := req.ParseForm(); err != nil {
					t.Errorf("Authenticate() sent a body that could not be parsed as a form: %s", err)
				}

				if got := len(req.Form["grant_type"]); got != 1 {
					t.Errorf("Authenticate() sent invalid number of grant_type values. got: %v, want: %v", got, 1)
				}
				if got := req.Form["grant_type"][0]; got != expectedGrantTypeValue {
					t.Errorf("Authenticate() sent invalid grant_type value. got: %v, want: %v", got, expectedGrantTypeValue)
				}

				if got := len(req.Form["client_assertion_type"]); got != 1 {
					t.Errorf("Authenticate() sent invalid number of client_assertion_type values. got: %v, want: %v", got, 1)
				}
				if got := req.Form["client_assertion_type"][0]; got != expectedClientAssertionTypeValue {
					t.Errorf("Authenticate() sent invalid client_assertion_type value. got: %v, want: %v", got, expectedClientAssertionTypeValue)
				}

				if len(tc.scopes) > 0 {
					if got := len(req.Form["scope"]); got != 1 {
						t.Errorf("Authenticate() sent invalid number of scope values. got: %v, want: %v", got, 1)
					}
					splitScopes := strings.Split(req.Form["scope"][0], " ")
					if diff := cmp.Diff(splitScopes, tc.scopes, cmpopts.SortSlices(func(a, b string) bool { return a > b })); diff != "" {
						t.Errorf("Authenticate() sent invalid scopes. diff: %s", diff)
					}
				}

				if got := len(req.Form["client_assertion"]); got != 1 {
					t.Errorf("Authenticate() sent invalid number of client_assertion_type values. got: %v, want: %v", got, 1)
				}
				claims := &jwt.StandardClaims{}
				token, err := jwt.ParseWithClaims(req.Form["client_assertion"][0], claims, func(_ *jwt.Token) (interface{}, error) {
					return key.Public(), nil
				})
				if err != nil {
					t.Fatalf("Failed to parse JWT: %v", err)
				}
				if token.Header["kid"].(string) != keyID {
					t.Errorf("Authenticate() sent invalid JWT key ID. got: %q; want %q", token.Header["kid"].(string), keyID)
				}
				if claims.Issuer != issuer {
					t.Errorf("Authenticate() sent incorrect 'iss' claim. got: %q; want %q", claims.Issuer, issuer)
				}
				if claims.Subject != subject {
					t.Errorf("Authenticate() sent incorrect 'sub' claim. got: %q; want %q", claims.Subject, subject)
				}
				if claims.Audience != authURL {
					t.Errorf("Authenticate() sent incorrect 'aud' claim. got: %q; want %q", claims.Audience, authURL)
				}
				if claims.ExpiresAt != now.Add(10*time.Minute).Unix() {
					t.Errorf("Authenticate() sent incorrect 'exp' claim. got: %d; want %d", claims.ExpiresAt, now.Add(10*time.Minute).Unix())
				}
				if !seenTokenIDs.Add(claims.Id) {
					t.Errorf("Authenticate() reused JWT ID %q", claims.Id)
				}

				w.Write([]byte(`{"access_token": "123", "expires_in": 1200}`))
			}))
			defer server.Close()

			authURL = server.URL + "/auth/token"

			authenticator, err := NewJWTOAuthAuthenticator(issuer, subject, authURL, &testKeyProvider{key, keyID}, &JWTOAuthOptions{Scopes: tc.scopes, JWTLifetime: 10 * time.Minute})
			if err != nil {
				t.Fatalf("NewJWTOAuthAuthenticator(%q, %q, %q, keyProvider, {Scopes: %v, JWTLifetime: 10 minutes}) error: %v", issuer, subject, authURL, tc.scopes, err)
			}

			buildRequestAndCheckHeader(t, authenticator, expectedHeader)
		})
	}
}

func TestJWTOAuthAuthenticator_Authenticate_WithError(t *testing.T) {
	wantErrBody := []byte(`an error`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(wantErrBody)
	}))
	defer server.Close()

	issuer := "issuer"
	subject := "subject"

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyID := uuid.New().String()

	authURL := server.URL + "/auth/token"
	authenticator, err := NewJWTOAuthAuthenticator(issuer, subject, authURL, &testKeyProvider{key, keyID}, nil)
	if err != nil {
		t.Fatalf("NewJWTOAuthAuthenticator(%q, %q, %q, keyProvider, nil) error: %v", issuer, subject, authURL, err)
	}
	if err := authenticator.Authenticate(http.DefaultClient); !errors.Is(err, ErrorUnexpectedStatusCode) {
		t.Errorf("Authenticate() returned unexpected error. got: %v, want: %v", err, ErrorUnexpectedStatusCode)
	}
}

func TestJWTOAuthAuthenticator_AuthenticateOnlyIfNecessary(t *testing.T) {
	for _, tc := range []struct {
		description      string
		responseTemplate string
		advanceTime      time.Duration
		opts             *JWTOAuthOptions
		wantAuthHeader   string
	}{
		{
			description:      "with expires_in, expiry not reached",
			responseTemplate: `{"access_token": "token%d", "expires_in": 1200}`,
			advanceTime:      5 * time.Minute,
			wantAuthHeader:   "Bearer token1",
		},
		{
			description:      "with expires_in, expiry reached",
			responseTemplate: `{"access_token": "token%d", "expires_in": 1200}`,
			advanceTime:      25 * time.Minute,
			wantAuthHeader:   "Bearer token2",
		},
		{
			description:      "no expires_in, default behaviour",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      25 * time.Minute,
			wantAuthHeader:   "Bearer token1",
		},
		{
			description:      "no expires_in, always authenticate",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      0,
			opts: &JWTOAuthOptions{
				AlwaysAuthenticateIfNoExpiresIn: true,
			},
			wantAuthHeader: "Bearer token2",
		},
		{
			description:      "no expires_in, default expiry not reached",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      5 * time.Minute,
			opts: &JWTOAuthOptions{
				DefaultExpiry: 20 * time.Minute,
			},
			wantAuthHeader: "Bearer token1",
		},
		{
			description:      "no expires_in, default expiry reached",
			responseTemplate: `{"access_token": "token%d"}`,
			advanceTime:      25 * time.Minute,
			opts: &JWTOAuthOptions{
				DefaultExpiry: 20 * time.Minute,
			},
			wantAuthHeader: "Bearer token2",
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			now := time.Now()
			timeNow = func() time.Time {
				return now
			}
			defer func() {
				timeNow = time.Now
			}()

			counter := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				counter++
				w.Write([]byte(fmt.Sprintf(tc.responseTemplate, counter)))
			}))
			defer server.Close()

			issuer := "issuer"
			subject := "subject"

			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			keyID := uuid.New().String()

			authURL := server.URL + "/auth/token"
			authenticator, err := NewJWTOAuthAuthenticator(issuer, subject, authURL, &testKeyProvider{key, keyID}, tc.opts)
			if err != nil {
				t.Fatalf("NewJWTOAuthAuthenticator(%q, %q, %q, keyProvider, nil) error: %v", issuer, subject, authURL, err)
			}

			buildRequestAndCheckHeader(t, authenticator, "Bearer token1")

			now = now.Add(tc.advanceTime)

			buildRequestAndCheckHeader(t, authenticator, tc.wantAuthHeader)
		})
	}
}

func buildRequestAndCheckHeader(t *testing.T, authenticator Authenticator, wantHeader string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, "some-url", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticator.AddAuthenticationToRequest(http.DefaultClient, req); err != nil {
		t.Fatalf("AddAuthenticationToRequest() returned unexpected error: %v", err)
	}
	authHeader := req.Header.Get(authorizationHeader)
	if authHeader != wantHeader {
		t.Fatalf("AddAuthenticationToRequest() added incorrect Authorization header: got %q, want: %q", authHeader, wantHeader)
	}
}
