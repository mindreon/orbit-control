//go:build e2e

package persistence

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var binaryPath string

// buildBinary compiles the real orbit-control entrypoint once per run.
func buildBinary() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "orbit-e2e-bin-")
	if err != nil {
		return err
	}
	binaryPath = filepath.Join(dir, "orbit-control")
	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/orbit-control")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}

// procReq is how a binary run appears in the report. Env holds names with
// display values only; URLs are never written.
type procReq struct {
	Binary string   `json:"binary"`
	Env    []string `json:"env"`
	Probe  string   `json:"probe"`
}

// envVar is one variable for the child. Display is what the report shows.
type envVar struct{ Name, Value, Display string }

func (e envVar) shown() string {
	d := e.Display
	if d == "" {
		d = e.Value
	}
	return e.Name + "=" + d
}

func dbURLVar(name, raw, display string) envVar {
	return envVar{Name: name, Value: raw, Display: display}
}

// withPassword swaps the password in a DB URL (for S-DB-9) or the port.
func withPassword(raw, password string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.UserPassword(u.User.Username(), password)
	return u.String()
}

func withPort(raw, port string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Host = net.JoinHostPort(u.Hostname(), port)
	return u.String()
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type proc struct {
	cmd    *exec.Cmd
	output *lockedBuffer
	done   chan struct{}
	err    error
	base   string
}

// startBinary runs orbit-control with a clean environment: only PATH/HOME
// and the given variables, so nothing leaks in from the test environment.
func startBinary(t *testing.T, vars []envVar) *proc {
	t.Helper()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	port := ""
	internalSet := false
	for _, v := range vars {
		env = append(env, v.Name+"="+v.Value)
		if v.Name == "PORT" {
			port = v.Value
		}
		internalSet = internalSet || v.Name == "ORBIT_INTERNAL_ADDR"
	}
	if !internalSet {
		// Parallel binaries must not share the default internal port.
		env = append(env, "ORBIT_INTERNAL_ADDR=127.0.0.1:"+freePort(t))
	}
	p := &proc{output: &lockedBuffer{}, done: make(chan struct{})}
	p.cmd = exec.Command(binaryPath)
	p.cmd.Env = env
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start orbit-control: %v", err)
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	if port != "" {
		p.base = "http://127.0.0.1:" + port
	}
	t.Cleanup(func() { p.stop() })
	return p
}

func (p *proc) stop() {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// waitExit reports the exit code, or -1 if still running after d.
func (p *proc) waitExit(d time.Duration) int {
	select {
	case <-p.done:
		if p.cmd.ProcessState != nil {
			return p.cmd.ProcessState.ExitCode()
		}
		return -2
	case <-time.After(d):
		return -1
	}
}

// waitHealthy polls /health until it answers 200 or the process exits.
func (p *proc) waitHealthy(d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		select {
		case <-p.done:
			return false
		case <-ctx.Done():
			return false
		default:
		}
		res, err := http.Get(p.base + "/health")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func shownEnv(vars []envVar) []string {
	out := make([]string, 0, len(vars))
	for _, v := range vars {
		if v.Name == "PORT" {
			out = append(out, "PORT=<free port>")
			continue
		}
		out = append(out, v.shown())
	}
	return out
}

func outputLines(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}
