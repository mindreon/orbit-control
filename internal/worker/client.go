package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 120 * time.Second},
	}
}

func (c *Client) Call(ctx context.Context, name string, payload any, out any) error {
	if c == nil || c.BaseURL == "" {
		return fmt.Errorf("worker URL is not configured")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/internal/activities/"+name, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("worker %s: HTTP %d: %s", name, res.StatusCode, string(raw))
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

type OpenSessionOut struct {
	SessionID string `json:"sessionId"`
}

type RunTurnOut struct {
	Status   string   `json:"status"`
	Approval *Ask     `json:"approval,omitempty"`
	Texts    []string `json:"texts,omitempty"`
}

type Ask struct {
	ApprovalRequestID string `json:"approvalRequestId"`
	ToolName          string `json:"toolName"`
	CallID            string `json:"callId,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type AppliedOut struct {
	Applied bool `json:"applied"`
}

type AbortedOut struct {
	Aborted bool `json:"aborted"`
}

type ClosedOut struct {
	Closed bool `json:"closed"`
}
