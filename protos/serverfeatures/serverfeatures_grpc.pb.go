// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.20.0
// source: serverfeatures.proto

package serverfeatures

import (
	context "context"
	shared "github.com/EventStore/EventStore-Client-Go/v3/protos/shared"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ServerFeaturesClient is the client API for ServerFeatures service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ServerFeaturesClient interface {
	GetSupportedMethods(ctx context.Context, in *shared.Empty, opts ...grpc.CallOption) (*SupportedMethods, error)
}

type serverFeaturesClient struct {
	cc grpc.ClientConnInterface
}

func NewServerFeaturesClient(cc grpc.ClientConnInterface) ServerFeaturesClient {
	return &serverFeaturesClient{cc}
}

func (c *serverFeaturesClient) GetSupportedMethods(ctx context.Context, in *shared.Empty, opts ...grpc.CallOption) (*SupportedMethods, error) {
	out := new(SupportedMethods)
	err := c.cc.Invoke(ctx, "/event_store.client.server_features.ServerFeatures/GetSupportedMethods", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ServerFeaturesServer is the server API for ServerFeatures service.
// All implementations must embed UnimplementedServerFeaturesServer
// for forward compatibility
type ServerFeaturesServer interface {
	GetSupportedMethods(context.Context, *shared.Empty) (*SupportedMethods, error)
	mustEmbedUnimplementedServerFeaturesServer()
}

// UnimplementedServerFeaturesServer must be embedded to have forward compatible implementations.
type UnimplementedServerFeaturesServer struct {
}

func (UnimplementedServerFeaturesServer) GetSupportedMethods(context.Context, *shared.Empty) (*SupportedMethods, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSupportedMethods not implemented")
}
func (UnimplementedServerFeaturesServer) mustEmbedUnimplementedServerFeaturesServer() {}

// UnsafeServerFeaturesServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ServerFeaturesServer will
// result in compilation errors.
type UnsafeServerFeaturesServer interface {
	mustEmbedUnimplementedServerFeaturesServer()
}

func RegisterServerFeaturesServer(s grpc.ServiceRegistrar, srv ServerFeaturesServer) {
	s.RegisterService(&ServerFeatures_ServiceDesc, srv)
}

func _ServerFeatures_GetSupportedMethods_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(shared.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerFeaturesServer).GetSupportedMethods(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/event_store.client.server_features.ServerFeatures/GetSupportedMethods",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerFeaturesServer).GetSupportedMethods(ctx, req.(*shared.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// ServerFeatures_ServiceDesc is the grpc.ServiceDesc for ServerFeatures service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ServerFeatures_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "event_store.client.server_features.ServerFeatures",
	HandlerType: (*ServerFeaturesServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetSupportedMethods",
			Handler:    _ServerFeatures_GetSupportedMethods_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "serverfeatures.proto",
}
