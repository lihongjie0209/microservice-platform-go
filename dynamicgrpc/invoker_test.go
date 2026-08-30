package dynamicgrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
)

type fakeConnections struct{ connection *grpc.ClientConn }

func (f fakeConnections) GRPC(name string) (*grpc.ClientConn, bool) {
	return f.connection, name == "health"
}

func TestInvokerReflectionJSONInvocation(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	invoker, err := New(fakeConnections{connection: connection})
	if err != nil {
		t.Fatal(err)
	}
	if err := invoker.Validate(t.Context(), "health", "/grpc.health.v1.Health/Check", `{"service":""}`); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	response, err := invoker.Invoke(t.Context(), "health", "/grpc.health.v1.Health/Check", `{"service":""}`)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if !strings.Contains(response, `"status": "SERVING"`) {
		t.Fatalf("response = %s", response)
	}
}

func TestInvokerRejectsUnconfiguredAndInvalidTargets(t *testing.T) {
	t.Parallel()
	invoker, err := New(fakeConnections{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invoker.Invoke(t.Context(), "missing", "/grpc.health.v1.Health/Check", `{}`); !errors.Is(err, ErrUpstreamNotConfigured) {
		t.Fatalf("Invoke() error = %v", err)
	}
	if ValidFullMethod("Health") || !ValidFullMethod("/grpc.health.v1.Health/Check") {
		t.Fatal("ValidFullMethod() accepted invalid or rejected valid method")
	}
}
