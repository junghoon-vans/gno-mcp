package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gnolang/gno/tm2/pkg/amino"
	ctypes "github.com/gnolang/gno/tm2/pkg/bft/rpc/core/types"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/gnoverse/gno-mcp/internal/profiles"
)

func TestRemoteMCPUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	startsRead := make(chan struct{}, 1)
	endsRead := make(chan struct{}, 1)
	var status ctypes.ResultStatus
	status.NodeInfo.Network = "test-fixture"
	statusJSON, err := amino.MarshalJSON(&status)
	require.NoError(t, err)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				Path string `json:"path"`
				Data []byte `json:"data"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid RPC", http.StatusBadRequest)
			return
		}
		switch {
		case req.Method == "status":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, statusJSON)
		case req.Params.Path == "vm/qrender" && string(req.Params.Data) == "gno.land/r/test/large:":
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush() // No Content-Length: the read itself must be bounded.
			io.WriteString(w, strings.Repeat("x", (1<<20)+1))
		case req.Params.Path == "vm/qrender" && string(req.Params.Data) == "gno.land/r/test/slow:":
			startsRead <- struct{}{}
			select {
			case <-r.Context().Done():
				endsRead <- struct{}{}
			case <-ctx.Done():
			}
		default:
			http.Error(w, "unexpected RPC", http.StatusBadRequest)
		}
	}))
	defer upstream.Close()

	cfg := &profiles.Config{Profiles: map[string]profiles.Profile{
		"staging": {RPCURL: upstream.URL, ChainID: "test-other"},
	}}
	server, err := openRemoteMCP(ctx, cfg, "staging", nil, false)
	require.Nil(t, server)
	require.ErrorContains(t, err, `expects chain "test-other", RPC reports "test-fixture"`)
	cfg.Profiles["staging"] = profiles.Profile{RPCURL: upstream.URL, ChainID: "test-fixture"}
	server, err = openRemoteMCP(ctx, cfg, "staging", nil, false)
	require.NoError(t, err)

	const token = "fixture"
	host := httptest.NewTLSServer(remoteMCPHandler(server, sha256.Sum256([]byte(token))))
	defer host.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{
		Endpoint:   host.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerTransport{host.Client().Transport, token}},
	}, nil)
	require.NoError(t, err)
	defer session.Close()
	result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "gno_render", Arguments: map[string]any{"realm": "gno.land/r/test/large"}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcpsdk.TextContent).Text, "RPC response exceeds 1 MiB")

	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, host.URL+"/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"gno_render","arguments":{"realm":"gno.land/r/test/slow"}}}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("MCP-Protocol-Version", "2025-11-25")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := host.Client().Do(request); err == nil {
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
		}
	}()
	select {
	case <-startsRead:
	case <-ctx.Done():
		t.Fatal("legacy MCP call did not reach the RPC")
	}
	cancelRequest()
	select {
	case <-endsRead:
	case <-ctx.Done():
		t.Fatal("HTTP cancellation did not reach the RPC")
	}
	<-done
}
