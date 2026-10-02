package onemediahub

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/oauth2"
)

const (
	o2URL                = "https://cloud.o2online.es"
	o2AuthURL            = "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/authorize"
	o2TokenURL           = "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/token"
	o2RedirectURL        = o2URL + "/ui/html/clientoauth.html"
	o2GermanyURL         = "https://cloud.o2.de"
	o2GermanyAuthURL     = "https://mondia-lcm.o2online.de/v2/web/auth/dialog/oauth"
	o2GermanyTokenURL    = "https://police.mondiamedia.com/v2/api/auth/token"
	o2GermanyRedirectURL = o2GermanyURL + "/ui/html/clientoauth.html"
	authOAuth            = "oauth"
	authPassword         = "password"
	authStateKey         = "config_auth_state"
	authVerifierKey      = "config_auth_verifier"
	credentialsKey       = "oauth_credentials"
	sessionKey           = "session"
	oauthPrefix          = "oauth "
	invalidKeyCode       = "SEC-1003"
)

func (o *options) oauthConfig() (*oauth2.Config, error) {
	secret := o.ClientSecret
	if secret != "" {
		var err error
		secret, err = obscure.Reveal(secret)
		if err != nil {
			return nil, fmt.Errorf("invalid client_secret: %w", err)
		}
	}
	c := &oauth2.Config{
		ClientID: o.ClientID, ClientSecret: secret,
		Endpoint:    oauth2.Endpoint{AuthURL: o.AuthURL, TokenURL: o.TokenURL},
		RedirectURL: o.RedirectURL, Scopes: strings.Fields(o.Scope),
	}
	var authURL, tokenURL, redirectURL string
	switch strings.TrimRight(o.URL, "/") {
	case o2URL:
		authURL, tokenURL, redirectURL = o2AuthURL, o2TokenURL, o2RedirectURL
		if o.Scope == "" {
			c.Scopes = []string{"openid"}
		}
	case o2GermanyURL:
		authURL, tokenURL, redirectURL = o2GermanyAuthURL, o2GermanyTokenURL, o2GermanyRedirectURL
	}
	if c.Endpoint.AuthURL == "" {
		c.Endpoint.AuthURL = authURL
	}

	if c.Endpoint.TokenURL == "" {
		c.Endpoint.TokenURL = tokenURL
	}

	if c.RedirectURL == "" {
		c.RedirectURL = redirectURL
	}
	return c, nil
}

func configure(ctx context.Context, name string, m configmap.Mapper, in fs.ConfigIn) (*fs.ConfigOut, error) {
	opt, err := readOptions(m)
	if err != nil {
		return nil, err
	}

	if opt.AuthType == authPassword {
		return nil, nil
	}
	c, err := opt.oauthConfig()
	if err != nil {
		return nil, err
	}

	switch in.State {
	case "":
		if token, _ := m.Get("token"); token != "" {
			return fs.ConfigConfirm("authorize", false, "config_refresh_token", "Replace the saved login?")
		}
		return fs.ConfigGoto("authorize")
	case "authorize":
		if in.Result == "false" {
			return nil, nil
		}

		if c.ClientID == "" || c.Endpoint.AuthURL == "" || c.Endpoint.TokenURL == "" || c.RedirectURL == "" {
			return nil, errors.New("OAuth requires client_id, auth_url, token_url and redirect_url; O2 supplies URL defaults")
		}
		for _, endpoint := range []string{c.Endpoint.AuthURL, c.Endpoint.TokenURL, c.RedirectURL} {
			if _, err := parseURL(endpoint); err != nil {
				return nil, err
			}
		}
		state, err := random.Password(32)
		if err != nil {
			return nil, err
		}
		verifier := oauth2.GenerateVerifier()
		m.Set(authStateKey, state)
		m.Set(authVerifierKey, verifier)
		args := []oauth2.AuthCodeOption{oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier)}
		switch strings.TrimRight(opt.URL, "/") {
		case o2URL:
			args = append(args, oauth2.SetAuthURLParam("acr_values", "2"))
		case o2GermanyURL:
			args = append(args, oauth2.SetAuthURLParam("client_type", "omh"))
		}
		loginURL := c.AuthCodeURL(state, args...)
		out, err := fs.ConfigInput("exchange", "config_callback", "Open this URL and sign in, then paste the complete final URL from your browser:\n\n"+loginURL)
		if out != nil {
			out.Option.Sensitive = true
		}
		return out, err
	case "exchange":
		callback, err := url.Parse(strings.TrimSpace(in.Result))
		if err != nil {
			return nil, errors.New("invalid callback URL")
		}
		expected, err := url.Parse(c.RedirectURL)
		if err != nil {
			return nil, errors.New("invalid redirect_url")
		}

		if callback.Scheme != expected.Scheme || callback.Host != expected.Host || callback.Path != expected.Path {
			return nil, errors.New("callback URL does not match redirect_url")
		}
		q := callback.Query()
		state, _ := m.Get(authStateKey)
		verifier, _ := m.Get(authVerifierKey)
		if state == "" || verifier == "" || q.Get("state") != state {
			return nil, errors.New("OAuth state mismatch; reconnect and try again")
		}

		if q.Get("error") != "" {
			return nil, errors.New("OAuth authorization was refused")
		}

		if q.Get("code") == "" {
			return nil, errors.New("callback URL has no authorization code")
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, newClient(ctx, opt))
		token, err := c.Exchange(ctx, q.Get("code"), oauth2.VerifierOption(verifier))
		if err != nil {
			return nil, fmt.Errorf("OAuth exchange failed: %w", err)
		}

		if token.RefreshToken == "" {
			return nil, errors.New("provider returned no refresh token; request offline access before reconnecting")
		}

		if err = oauthutil.PutToken(name, m, token, false); err != nil {
			return nil, err
		}
		m.Set(authStateKey, "")
		m.Set(authVerifierKey, "")
		m.Set(credentialsKey, "")
		m.Set(sessionKey, "")
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown configuration state %q", in.State)
	}
}

// auth serializes session renewal and protects rotating credentials.
type auth struct {
	mu         sync.Mutex
	name       string
	opt        *options
	m          configmap.Mapper
	srv        *rest.Client
	httpClient *http.Client
	session    api.Session
	token      *oauth2.Token
}

type storedSession struct {
	api.Session
	Scope string `json:"scope"` // Scope binds the session to its server and credentials.
}

func (a *auth) scope() string {
	// Session reuse requires the same server, client and credentials.
	values := []string{a.opt.URL, a.opt.APIPath, a.opt.AuthType, a.opt.User, a.opt.Password,
		a.opt.ClientID, a.opt.ClientSecret, a.opt.AuthURL, a.opt.TokenURL,
		a.opt.Platform, a.opt.MSISDN, a.opt.DeviceID, a.opt.UserAgent}
	if a.token != nil {
		values = append(values, a.token.AccessToken, a.token.RefreshToken)
	}
	b, _ := json.Marshal(values)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func (a *auth) restoreSession() {
	raw, _ := a.m.Get(sessionKey)
	var saved storedSession
	if json.Unmarshal([]byte(raw), &saved) == nil && saved.Scope == a.scope() && saved.ID != "" {
		a.session = saved.Session
	}
}

func (a *auth) saveSession() error {
	if a.session.ID == "" {
		return nil
	}
	b, err := json.Marshal(storedSession{Session: a.session, Scope: a.scope()})
	if err != nil {
		return err
	}

	if saved, _ := a.m.Get(sessionKey); saved != string(b) {
		a.m.Set(sessionKey, string(b))
	}
	return nil
}

func (a *auth) saveHeader(resp *http.Response, usedToken string) error {
	if resp == nil {
		return nil
	}
	header := resp.Header.Get("Authorization")
	if header == "" {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	// A delayed response must not overwrite credentials from a newer request.
	if a.token != nil && usedToken != a.token.AccessToken {
		return nil
	}
	return a.saveHeaderLocked(header)
}

func (a *auth) saveHeaderLocked(header string) error {
	scheme, encoded, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, authOAuth) {
		return errors.New("invalid SAPI OAuth response header")
	}
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		b, err = base64.RawStdEncoding.DecodeString(encoded)
	}

	if err != nil {
		return errors.New("invalid SAPI OAuth response encoding")
	}
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(b, &envelope) != nil {
		return errors.New("invalid SAPI OAuth response JSON")
	}
	var access, refresh string
	if json.Unmarshal(envelope.Data["accesstoken"], &access) != nil {
		return errors.New("SAPI OAuth response has no access token")
	}
	// O2 clients retain the current token when SAPI sends an empty replacement.
	if access == "" && a.token != nil {
		access = a.token.AccessToken
	}

	if access == "" {
		return errors.New("SAPI OAuth response has no access token")
	}
	token := &oauth2.Token{AccessToken: access, TokenType: "Bearer"}
	if a.token != nil {
		token.RefreshToken = a.token.RefreshToken
		token.Expiry = a.token.Expiry
	}

	if raw := envelope.Data["refreshtoken"]; raw != nil {
		if json.Unmarshal(raw, &refresh) != nil {
			return errors.New("invalid SAPI refresh token")
		}

		if refresh != "" {
			token.RefreshToken = refresh
		}
	}
	var expires json.Number
	if raw := envelope.Data["expiresin"]; raw != nil {
		if json.Unmarshal(raw, &expires) != nil {
			return errors.New("invalid SAPI token lifetime")
		}
		seconds, err := expires.Int64()
		if err != nil || seconds < 0 {
			return errors.New("invalid SAPI token lifetime")
		}
		// Providers may echo the original lifetime on every response.
		if token.Expiry.IsZero() || a.token == nil || a.token.AccessToken != access {
			token.Expiry = time.Now().Add(time.Duration(seconds) * time.Second)
		}
	} else if a.token == nil || a.token.AccessToken != access {
		token.Expiry = time.Time{}
	}

	if err := oauthutil.PutToken(a.name, a.m, token, false); err != nil {
		return err
	}
	a.token = token
	a.m.Set(credentialsKey, string(b))
	return a.saveSession()
}

func (a *auth) header() (string, error) {
	data := map[string]any{}
	if saved, _ := a.m.Get(credentialsKey); saved != "" {
		var envelope struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal([]byte(saved), &envelope); err != nil {
			return "", errors.New("invalid saved oauth_credentials")
		}

		if envelope.Data != nil {
			data = envelope.Data
		}
	}
	if data["accesstoken"] != a.token.AccessToken {
		delete(data, "expiresin")
		delete(data, "lastrefreshdate")
	}
	data["accesstoken"] = a.token.AccessToken
	data["refreshtoken"] = a.token.RefreshToken
	data["valid"] = a.token.Valid()
	data["platform"] = a.opt.Platform
	if a.opt.MSISDN != "" {
		data["msisdn"] = a.opt.MSISDN
	}
	b, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return "", err
	}
	return oauthPrefix + base64.StdEncoding.EncodeToString(b), nil
}

type authState struct {
	session     api.Session
	accessToken string
	header      string
}

func (a *auth) prepare(ctx context.Context) (authState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.opt.AuthType == authOAuth {
		token, err := oauthutil.GetToken(a.name, a.m)
		if err != nil {
			return authState{}, err
		}

		if a.token == nil || token.AccessToken != a.token.AccessToken {
			a.session = api.Session{}
		}
		a.token = token
		c, err := a.opt.oauthConfig()
		if err != nil {
			return authState{}, err
		}

		// Some providers delegate refresh to SAPI and do not expose client credentials.
		if !token.Valid() && c.ClientID != "" && c.Endpoint.TokenURL != "" {
			tokenCtx := context.WithValue(ctx, oauth2.HTTPClient, a.httpClient)
			token, err = c.TokenSource(tokenCtx, token).Token()
			if err != nil {
				return authState{}, fmt.Errorf("refresh OAuth token: %w", err)
			}

			if err := oauthutil.PutToken(a.name, a.m, token, false); err != nil {
				return authState{}, err
			}
			a.token = token
			a.session = api.Session{}
		}
	}
	if a.session.ID == "" {
		a.restoreSession()
	}

	if a.session.ID == "" {
		if err := a.login(ctx); err != nil {
			return authState{}, err
		}
	}
	state := authState{session: a.session}
	if a.token != nil {
		state.accessToken = a.token.AccessToken
		var err error
		state.header, err = a.header()
		if err != nil {
			return authState{}, err
		}
	}
	return state, nil
}

// login requires mu. Sessions are recreated without prompting the user.
func (a *auth) login(ctx context.Context) error {
	opts := rest.Opts{Method: http.MethodPost, Path: "/login", Parameters: url.Values{"action": {"login"}, "responsetime": {"true"}}, ContentType: "application/x-www-form-urlencoded; charset=UTF-8", NoRedirect: true}
	switch a.opt.AuthType {
	case authPassword:
		password, err := obscure.Reveal(a.opt.Password)
		if err != nil {
			return fmt.Errorf("invalid password: %w", err)
		}
		opts.Body = strings.NewReader(url.Values{"login": {a.opt.User}, "password": {password}}.Encode())
	case authOAuth:
		header, err := a.header()
		if err != nil {
			return err
		}
		opts.Path += "/oauth"
		opts.ExtraHeaders = map[string]string{"Authorization": header}
	}
	var reply api.Response
	resp, err := a.srv.CallJSON(ctx, &opts, nil, &reply)
	if resp != nil && resp.Header.Get("Authorization") != "" {
		if saveErr := a.saveHeaderLocked(resp.Header.Get("Authorization")); saveErr != nil {
			return saveErr
		}
	}
	if err != nil {
		return fmt.Errorf("OneMediaHub login: %w", err)
	}

	if reply.Error != nil {
		return reply.Error
	}
	var session api.Session
	if err := json.Unmarshal(reply.Data, &session); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}

	if session.ID == "" {
		return errors.New("login response has no session ID")
	}
	a.session = session
	return a.saveSession()
}

func (a *auth) invalidate(session api.Session) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.session == session {
		a.session = api.Session{}
		a.m.Set(sessionKey, "")
	}
}
