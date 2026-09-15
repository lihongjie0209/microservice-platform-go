// Package routepolicy provides stable route identities and compiled,
// database-owned authorization policy snapshots for platform services.
package routepolicy

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lihongjie0209/microservice-platform-go/stableid"
)

const RouteNamespace = "afc952de-95c3-455a-bc5c-89fb3c2a5da5"

var routeSegment = regexp.MustCompile(`^[a-z0-9_.-]+$`)

type Route struct {
	ID, Protocol, Method, Path, Operation, Description, ServiceName, SourceVersion string
}

func NewRoute(protocol, method, routePath, serviceName, sourceVersion string) (Route, error) {
	protocol, method = strings.ToLower(strings.TrimSpace(protocol)), strings.ToLower(strings.TrimSpace(method))
	routePath, serviceName = strings.TrimSpace(routePath), strings.ToLower(strings.TrimSpace(serviceName))
	if (protocol != "http" && protocol != "grpc") || method == "" || routePath == "" || serviceName == "" || !routeSegment.MatchString(serviceName) {
		return Route{}, fmt.Errorf("%w: incomplete route identity", ErrInvalid)
	}
	segments := []string{"route", serviceName, protocol, method}
	for _, segment := range strings.Split(strings.Trim(routePath, "/"), "/") {
		segment = strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(segment, ":"), "*"))
		if segment == "" || !routeSegment.MatchString(segment) {
			return Route{}, fmt.Errorf("%w: unsupported route segment %q", ErrInvalid, segment)
		}
		if strings.HasPrefix(segment, ".") || strings.HasPrefix(segment, "x-") {
			segment = "x-" + segment
		}
		segments = append(segments, segment)
	}
	generator, err := stableid.New(RouteNamespace)
	if err != nil {
		return Route{}, fmt.Errorf("create route id generator: %w", err)
	}
	id, err := generator.String(strings.Join(segments, ":"))
	if err != nil {
		return Route{}, fmt.Errorf("generate route id: %w", err)
	}
	return Route{ID: id, Protocol: protocol, Method: method, Path: routePath, ServiceName: serviceName, SourceVersion: sourceVersion}, nil
}
