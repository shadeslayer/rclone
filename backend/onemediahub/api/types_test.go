package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID(t *testing.T) {
	for _, raw := range []string{`123`, `"123"`, `9223372036854775807`} {
		var id ID
		require.NoError(t, json.Unmarshal([]byte(raw), &id))
		b, err := json.Marshal(id)
		require.NoError(t, err)
		assert.Equal(t, strings.Trim(raw, `"`), string(b))
		assert.NotContains(t, string(b), `"`)
	}
	for _, raw := range []string{`-1`, `"not-an-id"`, `1.5`, `true`} {
		var id ID
		assert.Error(t, json.Unmarshal([]byte(raw), &id))
	}
}
