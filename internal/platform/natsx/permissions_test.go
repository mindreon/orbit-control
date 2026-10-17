//go:build nats_integration

package natsx

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// This isolated broker uses the specification's appendix B permissions verbatim.
// It cannot affect the unprivileged test broker or any running v2 service.
const permissionConfig = `jetstream { store_dir: /data }
accounts {
 ORBIT {
  jetstream: enabled
  users: [
   { user: control, password: $ORBIT_NATS_CONTROL_PASSWORD,
    permissions: { publish: ["run.>", "$JS.API.STREAM.CREATE.RUN_EVENTS", "$JS.API.STREAM.UPDATE.RUN_EVENTS",
      "$JS.API.STREAM.INFO.RUN_EVENTS", "$JS.API.CONSUMER.CREATE.RUN_EVENTS.>",
      "$JS.API.CONSUMER.DURABLE.CREATE.RUN_EVENTS.>", "$JS.API.CONSUMER.INFO.RUN_EVENTS.>",
      "$JS.API.CONSUMER.MSG.NEXT.RUN_EVENTS.>", "$JS.ACK.RUN_EVENTS.>"], subscribe: ["_INBOX.>"] } },
   { user: worker, password: $ORBIT_NATS_WORKER_PASSWORD,
    permissions: { publish: ["run.>"], subscribe: ["_INBOX.>"] } },
   { user: gateway, password: $ORBIT_NATS_GATEWAY_PASSWORD,
    permissions: { publish: ["$JS.API.STREAM.INFO.RUN_EVENTS", "$JS.API.CONSUMER.CREATE.RUN_EVENTS.>",
      "$JS.API.CONSUMER.INFO.RUN_EVENTS.>", "$JS.API.CONSUMER.DELETE.RUN_EVENTS.>",
      "$JS.API.CONSUMER.MSG.NEXT.RUN_EVENTS.>"], subscribe: ["_INBOX.>"] } }
  ]
 }
}
`

func dockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "docker command failed")
	return strings.TrimSpace(string(raw))
}

func TestServicePermissions(t *testing.T) {
	if os.Getenv("ORBIT_TEST_NATS_URL") == "" {
		t.Skip("ORBIT_TEST_NATS_URL required")
	}
	name := "orbit-v2-test-permissions-" + ids.New()
	path := t.TempDir() + "/nats.conf"
	require.NoError(t, os.WriteFile(path, []byte(permissionConfig), 0600))
	password := "n" + ids.New()
	dockerCommand(t, "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1:0:4222",
		"-v", path+":/etc/nats/nats.conf:ro",
		"-e", "ORBIT_NATS_CONTROL_PASSWORD="+password, "-e", "ORBIT_NATS_WORKER_PASSWORD="+password,
		"-e", "ORBIT_NATS_GATEWAY_PASSWORD="+password, "nats:2.11.8-alpine", "-c", "/etc/nats/nats.conf")
	t.Cleanup(func() { dockerCommand(t, "rm", "-f", name) })
	address := dockerCommand(t, "port", name, "4222/tcp")
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	connect := func(user string) (*Bus, <-chan error) {
		async := make(chan error, 8)
		conn, err := nats.Connect("nats://"+address, nats.UserInfo(user, password), nats.Timeout(time.Second),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { async <- err }))
		require.NoError(t, err)
		t.Cleanup(conn.Close)
		js, err := jetstream.New(conn)
		require.NoError(t, err)
		return &Bus{Conn: conn, JS: js}, async
	}
	control, _ := connect("control")
	require.NoError(t, control.EnsureRunEventsStream(t.Context(), 0))
	consumer, err := control.EnsureSinkConsumer(t.Context())
	require.NoError(t, err)
	worker, workerErrors := connect("worker")
	gateway, gatewayErrors := connect("gateway")
	subject := "run." + ids.New() + "." + ids.New() + "." + ids.New()
	require.NoError(t, control.Publish(t.Context(), subject, "control", []byte(`{"source":"control"}`)))
	require.NoError(t, worker.Publish(t.Context(), subject, "worker", []byte(`{"source":"worker"}`)))
	// Control/sink can receive and acknowledge both service publications.
	for range 2 {
		msg, err := consumer.Next(jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		require.NoError(t, msg.Ack())
	}
	// Gateway can inspect and consume through an ordered ephemeral pull consumer.
	stream, err := gateway.JS.Stream(t.Context(), "RUN_EVENTS")
	require.NoError(t, err)
	_, err = stream.Info(t.Context())
	require.NoError(t, err)
	ordered, err := gateway.JS.OrderedConsumer(t.Context(), "RUN_EVENTS", jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject}, InactiveThreshold: 30 * time.Second})
	require.NoError(t, err)
	received := make(chan jetstream.Msg, 2)
	consuming, err := ordered.Consume(func(msg jetstream.Msg) { received <- msg }, jetstream.PullMaxMessages(1))
	require.NoError(t, err)
	defer consuming.Stop()
	for range 2 {
		select {
		case msg := <-received:
			require.Equal(t, subject, msg.Subject())
		case <-time.After(3 * time.Second):
			t.Fatal("gateway receive timeout")
		}
	}
	// E35 markers use the existing control publication and gateway consumer permissions.
	require.NoError(t, control.Publish(t.Context(), subject+".end", "end:run", []byte(`{"run_id":"run","event_id":"terminal"}`)))
	marker, err := gateway.JS.OrderedConsumer(t.Context(), "RUN_EVENTS", jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject + ".end"}, DeliverPolicy: jetstream.DeliverLastPolicy,
	})
	require.NoError(t, err)
	batch, err := marker.FetchNoWait(1)
	require.NoError(t, err)
	found := 0
	for msg := range batch.Messages() {
		require.Equal(t, subject+".end", msg.Subject())
		found++
	}
	require.NoError(t, batch.Error())
	require.Equal(t, 1, found)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err = worker.JS.CreateOrUpdateConsumer(ctx, "RUN_EVENTS", jetstream.ConsumerConfig{
		Durable: "forbidden", AckPolicy: jetstream.AckExplicitPolicy})
	require.Error(t, err)
	select {
	case err := <-workerErrors:
		require.Contains(t, strings.ToLower(err.Error()), "permissions violation")
	case <-time.After(time.Second):
		t.Fatal("worker consumer creation was not explicitly rejected")
	}
	ctx, cancel = context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err = gateway.JS.Publish(ctx, subject, []byte(`{}`))
	require.Error(t, err)
	select {
	case err := <-gatewayErrors:
		require.Contains(t, strings.ToLower(err.Error()), "permissions violation")
	case <-time.After(time.Second):
		t.Fatal("gateway publish was not explicitly rejected")
	}
	t.Log(fmt.Sprintf("appendix B: control publish/sink ack, worker publish, gateway ordered consume pass; forbidden APIs rejected"))
}
