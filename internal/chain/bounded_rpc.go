package chain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gnolang/gno/gno.land/pkg/gnoclient"
	rpcclient "github.com/gnolang/gno/tm2/pkg/bft/rpc/client"
	"github.com/gnolang/gno/tm2/pkg/bft/rpc/lib/types"
)

const maxReadRPCResponseBytes = 1 << 20

// NewBoundedReadClient uses a response-limited transport for status and ABCI reads.
// The standard Gno transport reads the entire response body before decoding it.
func NewBoundedReadClient(rpcURL, chainID string) (*Real, error) {
	u, err := url.Parse(rpcURL)
	if err != nil || !validReadRPCURL(u) {
		return nil, errors.New("RPC requires an absolute HTTP(S) URL without credentials")
	}
	if chainID == "" {
		return nil, errors.New("chain-id must not be empty")
	}
	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout:    5 * time.Second,
		IdleConnTimeout:        60 * time.Second,
		MaxResponseHeaderBytes: 16 << 10,
		DisableCompression:     true,
	}
	rpc := rpcclient.NewRPCClient(&boundedReadRPC{
		endpoint: u.String(),
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	})
	return &Real{cli: &gnoclient.Client{RPCClient: rpc}, chainID: chainID}, nil
}

func validReadRPCURL(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}

type boundedReadRPC struct {
	endpoint string
	client   *http.Client
}

func (c *boundedReadRPC) SendRequest(ctx context.Context, request rpctypes.RPCRequest) (*rpctypes.RPCResponse, error) {
	if request.Method != "status" && request.Method != "abci_query" {
		return nil, fmt.Errorf("unsupported read RPC method %q", request.Method)
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("RPC HTTP status %d", resp.StatusCode)
	}
	if resp.ContentLength > maxReadRPCResponseBytes {
		return nil, errors.New("RPC response exceeds 1 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReadRPCResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxReadRPCResponseBytes {
		return nil, errors.New("RPC response exceeds 1 MiB")
	}
	var result rpctypes.RPCResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	if result.ID != request.ID {
		if result.ID == nil && result.Error != nil {
			return nil, result.Error
		}
		return nil, errors.New("RPC response ID mismatch")
	}
	return &result, nil
}

func (*boundedReadRPC) SendBatch(context.Context, rpctypes.RPCRequests) (rpctypes.RPCResponses, error) {
	return nil, errors.New("remote read RPC does not support batches")
}

func (c *boundedReadRPC) Close() error {
	c.client.CloseIdleConnections()
	return nil
}
