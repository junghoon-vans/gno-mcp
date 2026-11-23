package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestStreamableHTTPS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "gnomcp", Version: "test"}, nil)
	mcpsdk.AddTool(server, &mcpsdk.Tool{Name: "status"}, func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, any, error) {
		return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "ready"}}}, nil, nil
	})
	host := httptest.NewTLSServer(streamableMCPHandler(server))
	defer host.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{
		Endpoint: host.URL + "/mcp", HTTPClient: host.Client(),
	}, nil)
	require.NoError(t, err)
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 1)
	require.Equal(t, "status", tools.Tools[0].Name)
	result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "status", Arguments: map[string]any{}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Equal(t, []mcpsdk.Content{&mcpsdk.TextContent{Text: "ready"}}, result.Content)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, host.URL+"/mcp", nil)
	require.NoError(t, err)
	request.Header.Set("Origin", "https://untrusted.example")
	request.Header.Set("Accept", "text/event-stream")
	resp, err := host.Client().Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}
