package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func streamableMCPHandler(server *mcpsdk.Server) http.Handler {
	mcp := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return server
	}, &mcpsdk.StreamableHTTPOptions{SessionTimeout: 30 * time.Minute})
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		if origins := r.Header.Values("Origin"); len(origins) != 0 {
			scheme := "http://"
			if r.TLS != nil {
				scheme = "https://"
			}
			if len(origins) != 1 || origins[0] != scheme+r.Host {
				http.Error(w, "invalid Origin", http.StatusForbidden)
				return
			}
		}
		mcp.ServeHTTP(w, r)
	})
	return mux
}

func runStreamableHTTP(ctx context.Context, server *mcpsdk.Server, addr, cert, key string) error {
	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil || !endpoint.Addr().IsLoopback() {
		return fmt.Errorf("-http-addr must be a numeric loopback address and port, got %q", addr)
	}

	listener, err := net.Listen("tcp", endpoint.String())
	if err != nil {
		return err
	}
	defer listener.Close()

	host := &http.Server{
		Handler:           streamableMCPHandler(server),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	scheme := "http"
	if cert != "" {
		scheme = "https"
	}
	log.Printf("MCP Streamable HTTP listening on %s://%s/mcp (local clients only)", scheme, listener.Addr())

	done := make(chan error, 1)
	go func() {
		if cert != "" {
			done <- host.ServeTLS(listener, cert, key)
			return
		}
		done <- host.Serve(listener)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := host.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			_ = host.Close()
		}
		serveErr := <-done
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}
