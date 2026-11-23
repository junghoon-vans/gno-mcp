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
	"sync/atomic"
	"testing"
	"time"

	"github.com/gnolang/gno/tm2/pkg/amino"
	abci "github.com/gnolang/gno/tm2/pkg/bft/abci/types"
	ctypes "github.com/gnolang/gno/tm2/pkg/bft/rpc/core/types"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/gnoverse/gno-mcp/internal/audit"
	"github.com/gnoverse/gno-mcp/internal/profiles"
)

func TestRemoteMCPUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	startsRead := make(chan struct{}, 1)
	endsRead := make(chan struct{}, 1)
	var blockStatus atomic.Bool
	var status ctypes.ResultStatus
	const realm = "gno.land/r/test/source"
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
			if blockStatus.Load() {
				startsRead <- struct{}{}
				select {
				case <-r.Context().Done():
					endsRead <- struct{}{}
				case <-ctx.Done():
				}
				return
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, statusJSON)
		case req.Params.Path == "vm/qfile" && string(req.Params.Data) == realm:
			writeABCIResult(t, w, req.ID, "source.gno\n")
		case req.Params.Path == "vm/qfile" && string(req.Params.Data) == realm+"/source.gno":
			writeABCIResult(t, w, req.ID, "package source\n\nfunc Counter() int { return 42 }\n")
		case req.Params.Path == "vm/qeval" && string(req.Params.Data) == realm+".Counter()":
			writeABCIResult(t, w, req.ID, "(42 int)")
		case req.Params.Path == "vm/qpaths" && string(req.Params.Data) == "gno.land/r/test/":
			writeABCIResult(t, w, req.ID, realm+"\n")
		case req.Params.Path == "vm/qrender" && string(req.Params.Data) == "gno.land/r/sys/cla:":
			writeABCIResult(t, w, req.ID, "CLA enforcement is currently DISABLED")
		case req.Params.Path == "vm/qrender" && string(req.Params.Data) == "gno.land/r/test/large:":
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush() // No Content-Length: the read itself must be bounded.
			io.WriteString(w, strings.Repeat("x", (1<<20)+1))
		default:
			http.Error(w, "unexpected RPC", http.StatusBadRequest)
		}
	}))
	defer upstream.Close()
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/graphql/query" {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, `{"data":{"transactions":[{"hash":"indexer-fixture","block_height":1,"messages":[{"value":{"__typename":"MsgCall"}}]}]}}`)
	}))
	defer indexer.Close()

	cfg := &profiles.Config{Profiles: map[string]profiles.Profile{
		"staging": {RPCURL: upstream.URL, ChainID: "test-other"},
	}}
	server, err := openRemoteMCP(ctx, cfg, "staging", nil, false)
	require.Nil(t, server)
	require.ErrorContains(t, err, `expects chain "test-other", RPC reports "test-fixture"`)
	cfg.Profiles["staging"] = profiles.Profile{RPCURL: upstream.URL, ChainID: "test-fixture", TxIndexerURL: indexer.URL + "/graphql/query"}
	server, err = openRemoteMCP(ctx, cfg, "staging", audit.NewLog(io.Discard), false)
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
	tools, err := session.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 10)
	for _, tc := range []struct {
		tool string
		args map[string]any
		want string
	}{
		{"gno_read", map[string]any{"path": realm, "full": true}, "func Counter() int"},
		{"gno_eval", map[string]any{"path": realm, "expr": "Counter()"}, "(42 int)"},
		{"gno_packages", map[string]any{"path": "gno.land/r/test/"}, realm},
		{"gno_status", map[string]any{}, "Node is up"},
		{"gno_profile_list", map[string]any{}, "1 profiles loaded"},
		{"gno_cla_info", map[string]any{"profile": "staging"}, "disabled"},
		{"gno_history", map[string]any{"realm": realm}, "indexer-fixture"},
		{"gno_activity", map[string]any{"realm": realm}, "indexer-fixture"},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: tc.tool, Arguments: tc.args})
			require.NoError(t, err)
			require.False(t, result.IsError, "%+v", result.Content)
			switch content := result.Content[0].(type) {
			case *mcpsdk.TextContent:
				require.Contains(t, content.Text, tc.want)
			case *mcpsdk.EmbeddedResource:
				require.Contains(t, content.Resource.Text, tc.want)
			default:
				t.Fatalf("unexpected content type %T", content)
			}
		})
	}

	result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "gno_render", Arguments: map[string]any{"realm": "gno.land/r/test/large"}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcpsdk.TextContent).Text, "RPC response exceeds 1 MiB")

	blockStatus.Store(true)
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, host.URL+"/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"gno_status","arguments":{}}}`))
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

func writeABCIResult(t *testing.T, w http.ResponseWriter, id json.RawMessage, data string) {
	t.Helper()
	result, err := amino.MarshalJSON(&ctypes.ResultABCIQuery{Response: abci.ResponseQuery{ResponseBase: abci.ResponseBase{Data: []byte(data)}}})
	if err != nil {
		t.Error(err)
		http.Error(w, "encode ABCI fixture", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, id, result)
}
