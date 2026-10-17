//go:build nats_integration

package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

func TestProcessSIGTERM(t *testing.T) {
	if os.Getenv("ORBIT_TEST_NATS_URL") == "" {
		t.Skip("ORBIT_TEST_NATS_URL required")
	}
	// The production binary fixes the stream name. Give the process its own
	// uniquely named broker, never create/delete RUN_EVENTS on the supplied bus.
	name := "orbit-v2-test-gateway-process-" + ids.New()
	docker := func(args ...string) string {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		raw, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		require.NoError(t, err, "isolated gateway test broker command failed")
		return strings.TrimSpace(string(raw))
	}
	docker("run", "-d", "--rm", "--name", name, "-p", "127.0.0.1:0:4222", "nats:2.11.8-alpine", "-js")
	t.Cleanup(func() { docker("rm", "-f", name) })
	address := docker("port", name, "4222/tcp")
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
	url := "nats://" + address
	bus, err := natsx.Connect(url, "orbit-v2-test-signal")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	claims := auth.StreamClaims{TenantID: ids.New(), SessionID: ids.New(), RunID: ids.New(), Scope: "run"}
	subject := "run." + claims.TenantID + "." + claims.SessionID + "." + claims.RunID
	_, err = bus.JS.CreateStream(t.Context(), jetstream.StreamConfig{Name: "RUN_EVENTS", Subjects: []string{subject}})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, bus.JS.DeleteStream(ctx, "RUN_EVENTS"))
	})
	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer, err := auth.NewStreamSigner(key, "test", nil)
	require.NoError(t, err)
	token, err := signer.Issue(claims)
	require.NoError(t, err)
	binary := t.TempDir() + "/stream-gateway"
	buildCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	raw, err := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".").CombinedOutput()
	require.NoError(t, err, string(raw))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address = listener.Addr().String()
	require.NoError(t, listener.Close())
	client := &http.Client{Timeout: 3 * time.Second}
	start := func() (*exec.Cmd, <-chan error) {
		cmd := exec.CommandContext(t.Context(), binary)
		cmd.Env = append(os.Environ(), "ORBIT_NATS_URL="+url, "ORBIT_STREAM_ADDR="+address,
			"ORBIT_STREAM_PUBLIC_KEYS=test:"+base64.StdEncoding.EncodeToString(pub), "ORBIT_STREAM_SHUTDOWN_GRACE=500ms")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		require.NoError(t, cmd.Start())
		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
			close(done)
		}()
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("gateway process did not exit")
			}
		})
		require.Eventually(t, func() bool {
			response, err := client.Get("http://" + address + "/healthz")
			if err != nil {
				return false
			}
			defer response.Body.Close()
			return response.StatusCode == 200
		}, 5*time.Second, 10*time.Millisecond)
		return cmd, done
	}
	connect := func(cursor string) *http.Response {
		req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+address+"/v1/runs/"+claims.RunID+"/events", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Last-Event-ID", cursor)
		response, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, response.StatusCode)
		return response
	}
	publish := func(n int) {
		raw := fmt.Appendf(nil, `{"v":1,"id":"%s:main:0:0:%d","tenant_id":"%s","session_id":"%s",`+
			`"run_id":"%s","member":"main","segment":0,"attempt":0,"type":"run.status",`+
			`"ts":"2026-10-16T00:00:00Z","data":{"status":"running"}}`,
			claims.RunID, n, claims.TenantID, claims.SessionID, claims.RunID)
		require.NoError(t, bus.Publish(t.Context(), subject, ids.New(), raw))
	}
	readID := func(scan *bufio.Scanner) string {
		for scan.Scan() {
			if id, ok := strings.CutPrefix(scan.Text(), "id: "); ok {
				return id
			}
		}
		require.NoError(t, scan.Err())
		t.Fatal("no event frame received")
		return ""
	}
	process, done := start()
	response := connect("0")
	scan := bufio.NewScanner(response.Body)
	publish(1)
	require.Equal(t, "1", readID(scan))
	require.NoError(t, process.Process.Signal(syscall.SIGTERM))
	publish(2)
	require.Equal(t, "2", readID(scan), "SIGTERM grace continues forwarding")
	for scan.Scan() {
		require.NotEqual(t, "event: end", scan.Text())
	}
	require.True(t, scan.Err() == nil || errors.Is(scan.Err(), io.ErrUnexpectedEOF), "unexpected shutdown error: %v", scan.Err())
	require.NoError(t, response.Body.Close())
	require.NoError(t, <-done)
	publish(3)
	_, _ = start()
	response = connect("2")
	defer response.Body.Close()
	require.Equal(t, "3", readID(bufio.NewScanner(response.Body)), "process restart resumes without a gap or duplicate")
}
