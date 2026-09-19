// Package postgrest implements the session.Store port against Supabase
// Postgres via the PostgREST REST API, using the service role key (the
// tables have RLS enabled with no policies — service-role-only access).
package postgrest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// APIError is a non-2xx PostgREST response. PostgREST puts the SQLSTATE in Code
// and, for the transaction functions, session_fail's machine-readable reason in
// Details; Message is the caller-facing prose.
type APIError struct {
	StatusCode int
	Code       string `json:"code"`
	Details    string `json:"details"`
	Message    string `json:"message"`
	Hint       string `json:"hint"`
}

func (e *APIError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("postgrest %d %s: %s", e.StatusCode, e.Code, e.Details)
	}
	return fmt.Sprintf("postgrest %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// Client reads and writes public.agent_sessions / public.agent_messages
// through the Supabase PostgREST REST API. Access requires the service role
// key; the tables have no anon/authenticated policies.
type Client struct {
	url        string
	serviceKey string
	client     *http.Client
}

// NewClient returns a Client backed by Supabase PostgREST; it implements both
// session.Store (v1) and session.DurableStore (v2). A nil httpClient uses
// http.DefaultClient.
func NewClient(url, serviceKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{url: url, serviceKey: serviceKey, client: httpClient}
}

// do performs one PostgREST request and decodes the JSON body into out
// (skipped when out is nil). prefer sets the PostgREST Prefer header.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, prefer string, out any) error {
	u := c.url + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("apikey", c.serviceKey)
	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// PostgREST reports the failure as JSON; surface its machine-readable
		// reason so callers can map it to a domain error.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		apiErr := &APIError{StatusCode: resp.StatusCode}
		if err := json.Unmarshal(body, apiErr); err != nil || (apiErr.Message == "" && apiErr.Code == "") {
			return &APIError{StatusCode: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
		}
		return apiErr
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}
