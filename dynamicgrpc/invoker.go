// Package dynamicgrpc invokes allow-listed unary gRPC methods from JSON by
// using server reflection. It is intended for platform orchestration such as
// schedulers and workflow service tasks, where generating every downstream
// client would couple release cycles.
package dynamicgrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"    //nolint:staticcheck // grpcurl exposes v1 descriptors.
	"github.com/jhump/protoreflect/dynamic" //nolint:staticcheck // grpcurl's JSON parser requires v1 dynamic messages.
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
)

var ErrUpstreamNotConfigured = errors.New("gRPC upstream is not configured")

type ConnectionRegistry interface {
	GRPC(string) (*grpc.ClientConn, bool)
}

type Invoker struct {
	registry ConnectionRegistry
}

func New(registry ConnectionRegistry) (*Invoker, error) {
	if registry == nil {
		return nil, errors.New("gRPC connection registry is required")
	}
	return &Invoker{registry: registry}, nil
}

func (i *Invoker) Validate(ctx context.Context, upstream, fullMethod, requestJSON string) error {
	_, source, closeReflection, err := i.resolve(ctx, upstream, fullMethod)
	if err != nil {
		return err
	}
	defer closeReflection()
	parser, _, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, source, strings.NewReader(requestJSON), grpcurl.FormatOptions{})
	if err != nil {
		return fmt.Errorf("create dynamic JSON parser: %w", err)
	}
	method, err := unaryMethod(source, fullMethod)
	if err != nil {
		return err
	}
	request := dynamic.NewMessage(method.GetInputType())
	if err := parser.Next(request); err != nil {
		return fmt.Errorf("decode request JSON for %q: %w", fullMethod, err)
	}
	if err := parser.Next(dynamic.NewMessage(method.GetInputType())); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request JSON for %q must contain exactly one message", fullMethod)
	}
	return nil
}

func (i *Invoker) Invoke(ctx context.Context, upstream, fullMethod, requestJSON string) (string, error) {
	connection, source, closeReflection, err := i.resolve(ctx, upstream, fullMethod)
	if err != nil {
		return "", err
	}
	defer closeReflection()
	parser, formatter, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, source, strings.NewReader(requestJSON), grpcurl.FormatOptions{})
	if err != nil {
		return "", fmt.Errorf("create dynamic request codec: %w", err)
	}
	var output bytes.Buffer
	handler := &grpcurl.DefaultEventHandler{Out: &output, Formatter: formatter}
	if err := grpcurl.InvokeRPC(ctx, source, connection, strings.TrimPrefix(fullMethod, "/"), nil, handler, parser.Next); err != nil {
		return "", fmt.Errorf("invoke dynamic gRPC method %q: %w", fullMethod, err)
	}
	if handler.Status != nil && handler.Status.Err() != nil {
		return "", handler.Status.Err()
	}
	if handler.NumResponses != 1 {
		return "", fmt.Errorf("unary gRPC method %q returned %d responses", fullMethod, handler.NumResponses)
	}
	return strings.TrimSpace(output.String()), nil
}

func (i *Invoker) resolve(ctx context.Context, upstream, fullMethod string) (*grpc.ClientConn, grpcurl.DescriptorSource, func(), error) {
	connection, ok := i.registry.GRPC(strings.TrimSpace(upstream))
	if !ok {
		return nil, nil, func() {}, fmt.Errorf("%w: %s", ErrUpstreamNotConfigured, upstream)
	}
	if !ValidFullMethod(fullMethod) {
		return nil, nil, func() {}, fmt.Errorf("invalid full gRPC method %q", fullMethod)
	}
	reflectionClient := grpcreflect.NewClientAuto(ctx, connection)
	source := grpcurl.DescriptorSourceFromServer(ctx, reflectionClient)
	if _, err := unaryMethod(source, fullMethod); err != nil {
		reflectionClient.Reset()
		return nil, nil, func() {}, fmt.Errorf("resolve gRPC method %q through reflection: %w", fullMethod, err)
	}
	return connection, source, reflectionClient.Reset, nil
}

func unaryMethod(source grpcurl.DescriptorSource, fullMethod string) (*desc.MethodDescriptor, error) {
	descriptor, err := source.FindSymbol(symbolName(fullMethod))
	if err != nil {
		return nil, fmt.Errorf("resolve gRPC method %q: %w", fullMethod, err)
	}
	method, ok := descriptor.(*desc.MethodDescriptor)
	if !ok || method.IsClientStreaming() || method.IsServerStreaming() {
		return nil, fmt.Errorf("gRPC target %q must be a unary method", fullMethod)
	}
	return method, nil
}

func ValidFullMethod(value string) bool {
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(value), "/"), "/")
	return len(parts) == 2 && strings.Contains(parts[0], ".") && parts[1] != ""
}

func symbolName(fullMethod string) string {
	return strings.Replace(strings.TrimPrefix(strings.TrimSpace(fullMethod), "/"), "/", ".", 1)
}
