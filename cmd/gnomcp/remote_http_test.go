package main

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/gnoverse/gno-mcp/internal/chain"
	"github.com/gnoverse/gno-mcp/internal/profiles"
)

func TestRemoteMCP(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const token = "test-service-credential"
	const realm = "gno.land/r/test/example"
	fake := chain.NewFake()
	fake.SetRender(realm, "", "remote fixture")
	server, err := newRemoteMCP("testnet", profiles.BuiltinProfiles()["testnet"], fake, nil, false)
	require.NoError(t, err)
	handler := remoteMCPHandler(server, sha256.Sum256([]byte(token)))
	local := httptest.NewServer(handler)
	defer local.Close()
	tlsHost := httptest.NewTLSServer(handler)
	defer tlsHost.Close()
	for _, scenario := range []struct {
		name   string
		host   *httptest.Server
		token  string
		status int
	}{
		{"no TLS", local, token, http.StatusUpgradeRequired},
		{"missing token", tlsHost, "", http.StatusUnauthorized},
		{"wrong token", tlsHost, "incorrect", http.StatusUnauthorized},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, scenario.host.URL+"/mcp", nil)
			require.NoError(t, err)
			if scenario.token != "" {
				req.Header.Set("Authorization", "Bearer "+scenario.token)
			}
			resp, err := scenario.host.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, scenario.status, resp.StatusCode)
		})
	}

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{
		Endpoint: tlsHost.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerTransport{tlsHost.Client().Transport, token}},
	}, nil)
	require.NoError(t, err)
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	require.NoError(t, err)
	var names []string
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	require.ElementsMatch(t, []string{"gno_account", "gno_render"}, names)
	result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "gno_render", Arguments: map[string]any{"realm": realm}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcpsdk.TextContent).Text, "remote fixture")
	result, err = session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "gno_render", Arguments: map[string]any{"realm": realm, "profile": "testnet"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcpsdk.TextContent).Text, "remote fixture")
	for _, request := range []*mcpsdk.CallToolParams{
		{Name: "gno_call", Arguments: map[string]any{}},
		{Name: "gno_render", Arguments: map[string]any{"realm": realm, "profile": "mainnet"}},
	} {
		result, err := session.CallTool(ctx, request)
		if err == nil {
			require.True(t, result.IsError, request.Name)
		}
	}
}

type bearerTransport struct {
	next  http.RoundTripper
	token string
}

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(req)
}
