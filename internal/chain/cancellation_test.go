package chain

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRealReadRPCCancellation(t *testing.T) {
	const realm = "gno.land/r/demo/counter"
	for _, tc := range []struct {
		name string
		call func(context.Context, *Real) error
	}{
		{"Render", func(ctx context.Context, r *Real) error { _, err := r.Render(ctx, realm, ""); return err }},
		{"Eval", func(ctx context.Context, r *Real) error { _, err := r.Eval(ctx, realm, "Counter()"); return err }},
		{"File", func(ctx context.Context, r *Real) error { _, err := r.File(ctx, realm, "counter.gno"); return err }},
		{"ListFiles", func(ctx context.Context, r *Real) error { _, err := r.ListFiles(ctx, realm); return err }},
		{"ListPaths", func(ctx context.Context, r *Real) error { _, err := r.ListPaths(ctx, realm, 10); return err }},
		{"Account", func(ctx context.Context, r *Real) error {
			_, err := r.Account(ctx, "g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5")
			return err
		}},
		{"PackageMeta", func(ctx context.Context, r *Real) error { _, err := r.PackageMeta(ctx, realm); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			guard, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			started := make(chan struct{})
			canceled := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-req.Context().Done():
					close(canceled)
				case <-guard.Done():
				}
			}))
			defer upstream.Close()
			client, err := NewReal(upstream.URL, "test-fixture")
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(guard)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- tc.call(ctx, client) }()
			select {
			case <-started:
				cancel()
			case <-guard.Done():
				t.Fatal("read never reached the RPC server")
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-guard.Done():
				t.Fatal("read did not return after cancellation")
			}
			select {
			case <-canceled:
			case <-guard.Done():
				t.Fatal("RPC connection did not observe cancellation")
			}
		})
	}
}
