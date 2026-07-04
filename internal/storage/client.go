// Package storage is a thin SeaweedFS filer HTTP client.
//
// SeaweedFS filer semantics used:
//   PUT/POST /path                        upload (server may reply 201 or 204)
//   GET      /path                        fetch (streaming)
//   HEAD     /path                        metadata (Content-Length, ETag)
//   DELETE   /path                        remove
//
// Copy is done as GET-then-PUT because the mainline filer does not expose an
// atomic copy endpoint we can rely on across versions.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to a single SeaweedFS filer base URL.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a filer client. baseURL should be like "http://seaweedfs:8888".
func New(baseURL string) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 0, // no total timeout — uploads can be long. Use context deadlines.
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// BaseURL returns the configured filer base URL.
func (c *Client) BaseURL() string { return c.baseURL }

func (c *Client) urlFor(key string) string {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	// Percent-encode each path segment but keep the '/' separators.
	parts := strings.Split(key, "/")
	for i, seg := range parts {
		parts[i] = url.PathEscape(seg)
	}
	return c.baseURL + strings.Join(parts, "/")
}

// Put uploads (or replaces) an object at key. body must be readable.
func (c *Client) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.urlFor(key), body)
	if err != nil {
		return err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("seaweed PUT %s: %s", key, resp.Status)
}

// Delete removes an object. 404 is treated as success.
func (c *Client) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.urlFor(key), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 300 || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("seaweed DELETE %s: %s", key, resp.Status)
}

// Head does a HEAD request. Returns (nil, false, nil) on 404.
func (c *Client) Head(ctx context.Context, key string) (http.Header, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.urlFor(key), nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("seaweed HEAD %s: %s", key, resp.Status)
	}
	return resp.Header, true, nil
}

// Get returns the raw response for streaming. Caller MUST close body.
// Additional request headers may be provided (e.g. Range).
func (c *Client) Get(ctx context.Context, key string, extraHeaders http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.urlFor(key), nil)
	if err != nil {
		return nil, err
	}
	for k, vs := range extraHeaders {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return c.http.Do(req)
}

// Copy performs a GET → PUT to duplicate an object. Streams (no full buffer).
// Content-Type is preserved from the source Content-Type header when present.
func (c *Client) Copy(ctx context.Context, srcKey, dstKey string) error {
	resp, err := c.Get(ctx, srcKey, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("seaweed GET (copy src) %s: %s", srcKey, resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	size := resp.ContentLength
	return c.Put(ctx, dstKey, resp.Body, size, ct)
}

// Ping checks the filer is reachable. It deliberately uses HEAD on a path
// that is guaranteed not to exist (`/.gateway-ping-{unlikely}`); SeaweedFS
// answers 404 quickly without rendering the directory listing template.
// A GET on "/" would trigger the filer's HTML directory renderer, which
// then logs a `broken pipe` every time we close the response early —
// harmless but very noisy.
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.baseURL+"/.gateway-ping", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	// Drain + close so keep-alive can reuse the conn.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// Any answer that isn't a 5xx means the filer is reachable and speaking
	// HTTP. 404 is the expected, correct reply.
	if resp.StatusCode >= 500 {
		return fmt.Errorf("seaweed ping %s: %s", c.baseURL, resp.Status)
	}
	return nil
}
