package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/gnoverse/gno-mcp/internal/audit"
	"github.com/gnoverse/gno-mcp/internal/chain"
	"github.com/gnoverse/gno-mcp/internal/profiles"
	"github.com/gnoverse/gno-mcp/internal/server"
	readtools "github.com/gnoverse/gno-mcp/internal/tools/read"
)

const defaultRemoteProfile = "mainnet"

func loadMCPToken(path string) ([sha256.Size]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer file.Close()
	var empty [sha256.Size]byte
	const hexSize = sha256.Size * 2
	data, err := io.ReadAll(io.LimitReader(file, hexSize+3))
	if err != nil {
		return empty, err
	}
	token := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if len(token) != hexSize {
		return empty, errors.New("MCP token must be 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return empty, errors.New("MCP token must be 64 hexadecimal characters")
	}
	return sha256.Sum256([]byte(token)), nil
}

func newRemoteMCP(profileName string, profile profiles.Profile, client chain.Client, auditLog *audit.Log, auditReads bool) (*mcpsdk.Server, error) {
	cfg := &profiles.Config{Profiles: map[string]profiles.Profile{profileName: profile}}
	s := server.NewServer(cfg, "")
	resolve := func(name string) chain.Client {
		if name == profileName {
			return client
		}
		return nil
	}
	readtools.RegisterAccount(s, resolve)
	readtools.RegisterRender(s, resolve)
	mcp := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "gnomcp", Version: version}, &mcpsdk.ServerOptions{
		Instructions: fmt.Sprintf("Read-only Gno access on profile %q (chain %q). Use gno_account for public account state and gno_render for rendered realm content.", profileName, profile.ChainID),
	})
	// Hold capacity until the RPC returns, even if its HTTP client disconnects.
	inFlight := make(chan struct{}, 16)
	mcp.AddReceivingMiddleware(func(next mcpsdk.MethodHandler) mcpsdk.MethodHandler {
		return func(ctx context.Context, method string, req mcpsdk.Request) (mcpsdk.Result, error) {
			if method != "tools/call" {
				return next(ctx, method, req)
			}
			select {
			case inFlight <- struct{}{}:
				defer func() { <-inFlight }()
				return next(ctx, method, req)
			default:
				return &mcpsdk.CallToolResult{IsError: true, Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: "server is busy; retry later"},
				}}, nil
			}
		}
	})
	if err := publishTools(mcp, s, auditLog, auditReads); err != nil {
		return nil, err
	}
	return mcp, nil
}

func openRemoteMCP(cfg *profiles.Config, requestedProfile string, auditLog *audit.Log, auditReads bool) (*mcpsdk.Server, error) {
	name := cmp.Or(requestedProfile, defaultRemoteProfile)
	profile, ok := cfg.Profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown remote MCP profile %q", name)
	}
	if name == defaultRemoteProfile && profile.ChainID != profiles.BuiltinProfiles()[name].ChainID {
		return nil, errors.New("the mainnet profile must use the built-in mainnet chain ID")
	}
	client, err := chain.NewReal(profile.RPCURL, profile.ChainID)
	if err != nil {
		return nil, fmt.Errorf("RPC for profile %q: %w", name, err)
	}
	return newRemoteMCP(name, profile, client, auditLog, auditReads)
}

func remoteMCPHandler(server *mcpsdk.Server, tokenHash [sha256.Size]byte) http.Handler {
	mcp := streamableMCPMux(server, &mcpsdk.StreamableHTTPOptions{
		Stateless: true, MaxRequestBodyBytes: 64 << 10, PropagateRequestCancellation: true,
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			http.Error(w, "HTTPS required", http.StatusUpgradeRequired)
			return
		}
		values := r.Header.Values("Authorization")
		if len(values) == 1 {
			scheme, token, ok := strings.Cut(values[0], " ")
			if ok && strings.EqualFold(scheme, "Bearer") && validMCPToken(token, tokenHash) {
				mcp.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="gnomcp"`)
		http.Error(w, "invalid or missing credentials", http.StatusUnauthorized)
	})
}

func validMCPToken(token string, expected [sha256.Size]byte) bool {
	actual := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
}
