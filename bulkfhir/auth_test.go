package bulkfhir

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"access_token": "123", "expires_in": 1200}`))
			}))
			defer server.Close()
			authURL := server.URL + "/auth/token"

			authenticator, err := NewHTTPBasicOAuthAuthenticator(clientID, clientSecret, authURL, &HTTPBasicOAuthOptions{Scopes: tc.scopes})
			if err != nil {
				t.Fatalf("NewHTTPBasicOAuthAuthenticator(%q, %q, %q, {Scopes: %v}) error: %v", clientID, clientSecret, authURL, tc.scopes, err)
			}

			checkAuthHeader(t, authenticator, expectedHeader)
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
				w.WriteHeader(http.StatusOK)
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

			checkAuthHeader(t, authenticator, "Bearer token1")

			now = now.Add(tc.advanceTime)

			checkAuthHeader(t, authenticator, tc.wantAuthHeader)
		})
	}
}

func checkAuthHeader(t *testing.T, authenticator Authenticator, wantHeader string) {
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
