package onemediahub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestOAuthSAPIRefresh(t *testing.T) {
	var exchanges, logins, reads atomic.Int32
	saved := `{"data":{"accesstoken":"access-old","refreshtoken":"refresh-old","expiresin":3600,"extension":"provider-value"}}`
	rotated := `{"data":{"accesstoken":"access-new","refreshtoken":"refresh-new","expiresin":3600,"extension":"provider-value"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
		case "/token":
			exchanges.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			jsonReply(t, w, map[string]string{"error": "invalid_request", "error_description": "Invalid Token"})
		case "/sapi/login/oauth":
			logins.Add(1)
			encoded := r.Header.Get("Authorization")
			require.True(t, len(encoded) >= len(oauthPrefix))
			b, err := base64.StdEncoding.DecodeString(encoded[len(oauthPrefix):])
			require.NoError(t, err)
			var envelope struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(b, &envelope))
			assert.Equal(t, "access-old", envelope.Data["accesstoken"])
			assert.Equal(t, "refresh-old", envelope.Data["refreshtoken"])
			assert.Equal(t, "provider-value", envelope.Data["extension"])
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(rotated)))
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/profile":
			reads.Add(1)
			cookie, err := r.Cookie("JSESSIONID")
			require.NoError(t, err)
			assert.Equal(t, "session", cookie.Value)
			assert.Equal(t, "key", r.URL.Query().Get("validationkey"))
			b, err := base64.StdEncoding.DecodeString(r.Header.Get("Authorization")[len(oauthPrefix):])
			require.NoError(t, err)
			assert.Contains(t, string(b), `"accesstoken":"access-new"`)
			jsonReply(t, w, map[string]any{"data": map[string]string{}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "token_url": srv.URL + "/token", "client_id": "client", credentialsKey: saved})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access-old", RefreshToken: "refresh-old", Expiry: time.Now().Add(-time.Hour)}, false))
	_, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "access-new", token.AccessToken)
	assert.Equal(t, "refresh-new", token.RefreshToken)
	assert.True(t, token.Valid())
	assert.JSONEq(t, rotated, m[credentialsKey])

	// A saved SAPI session remains usable when the provider token expires.
	token.Expiry = time.Now().Add(-time.Hour)
	require.NoError(t, oauthutil.PutToken("test", m, token, false))
	remote, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = remote.(*Fs).request(context.Background(), http.MethodGet, "/profile", "get", nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, exchanges.Load())
	assert.EqualValues(t, 1, logins.Load())
	assert.EqualValues(t, 1, reads.Load())
}
