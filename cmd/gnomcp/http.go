package main

import (
	"context"
	"crypto/tls"
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
	return streamableMCPMux(server, &mcpsdk.StreamableHTTPOptions{SessionTimeout: 30 * time.Minute})
}

func streamableMCPMux(server *mcpsdk.Server, options *mcpsdk.StreamableHTTPOptions) http.Handler {
	mcp := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return server
	}, options)
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

func mcpListenAddress(addr string, authenticated bool) (netip.AddrPort, error) {
	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil {
		return endpoint, fmt.Errorf("-http-addr must be a numeric IP address and port, got %q", addr)
	}
	if !authenticated && !endpoint.Addr().IsLoopback() {
		return endpoint, errors.New("non-loopback -http-addr requires -http-token-file and TLS")
	}
	return endpoint, nil
}

func runStreamableHTTP(ctx context.Context, handler http.Handler, endpoint netip.AddrPort, cert, key string, authenticated bool) error {
	host := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	if authenticated {
		host.ReadTimeout = 10 * time.Second
		host.WriteTimeout = 75 * time.Second // upstream RPC has a 60-second deadline
		host.IdleTimeout = 60 * time.Second
		host.MaxHeaderBytes = 16 << 10
	}

	scheme := "http"
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return err
		}
		host.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
		scheme = "https"
	}

	listener, err := net.Listen("tcp", endpoint.String())
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("MCP Streamable HTTP listening on %s://%s/mcp", scheme, listener.Addr())

	done := make(chan error, 1)
	go func() {
		if cert != "" {
			done <- host.ServeTLS(listener, "", "")
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
