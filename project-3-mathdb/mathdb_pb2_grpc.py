# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import mathdb_pb2 as mathdb__pb2


class MathDbStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Set = channel.unary_unary(
                '/MathDb/Set',
                request_serializer=mathdb__pb2.SetRequest.SerializeToString,
                response_deserializer=mathdb__pb2.SetResponse.FromString,
                )
        self.Get = channel.unary_unary(
                '/MathDb/Get',
                request_serializer=mathdb__pb2.GetRequest.SerializeToString,
                response_deserializer=mathdb__pb2.GetResponse.FromString,
                )
        self.Add = channel.unary_unary(
                '/MathDb/Add',
                request_serializer=mathdb__pb2.BinaryOpRequest.SerializeToString,
                response_deserializer=mathdb__pb2.BinaryOpResponse.FromString,
                )
        self.Sub = channel.unary_unary(
                '/MathDb/Sub',
                request_serializer=mathdb__pb2.BinaryOpRequest.SerializeToString,
                response_deserializer=mathdb__pb2.BinaryOpResponse.FromString,
                )
        self.Mult = channel.unary_unary(
                '/MathDb/Mult',
                request_serializer=mathdb__pb2.BinaryOpRequest.SerializeToString,
                response_deserializer=mathdb__pb2.BinaryOpResponse.FromString,
                )
        self.Div = channel.unary_unary(
                '/MathDb/Div',
                request_serializer=mathdb__pb2.BinaryOpRequest.SerializeToString,
                response_deserializer=mathdb__pb2.BinaryOpResponse.FromString,
                )


class MathDbServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Set(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Get(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Add(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Sub(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Mult(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Div(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_MathDbServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Set': grpc.unary_unary_rpc_method_handler(
                    servicer.Set,
                    request_deserializer=mathdb__pb2.SetRequest.FromString,
                    response_serializer=mathdb__pb2.SetResponse.SerializeToString,
            ),
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=mathdb__pb2.GetRequest.FromString,
                    response_serializer=mathdb__pb2.GetResponse.SerializeToString,
            ),
            'Add': grpc.unary_unary_rpc_method_handler(
                    servicer.Add,
                    request_deserializer=mathdb__pb2.BinaryOpRequest.FromString,
                    response_serializer=mathdb__pb2.BinaryOpResponse.SerializeToString,
            ),
            'Sub': grpc.unary_unary_rpc_method_handler(
                    servicer.Sub,
                    request_deserializer=mathdb__pb2.BinaryOpRequest.FromString,
                    response_serializer=mathdb__pb2.BinaryOpResponse.SerializeToString,
            ),
            'Mult': grpc.unary_unary_rpc_method_handler(
                    servicer.Mult,
                    request_deserializer=mathdb__pb2.BinaryOpRequest.FromString,
                    response_serializer=mathdb__pb2.BinaryOpResponse.SerializeToString,
            ),
            'Div': grpc.unary_unary_rpc_method_handler(
                    servicer.Div,
                    request_deserializer=mathdb__pb2.BinaryOpRequest.FromString,
                    response_serializer=mathdb__pb2.BinaryOpResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'MathDb', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class MathDb(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Set(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Set',
            mathdb__pb2.SetRequest.SerializeToString,
            mathdb__pb2.SetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Get',
            mathdb__pb2.GetRequest.SerializeToString,
            mathdb__pb2.GetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Add(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Add',
            mathdb__pb2.BinaryOpRequest.SerializeToString,
            mathdb__pb2.BinaryOpResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Sub(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Sub',
            mathdb__pb2.BinaryOpRequest.SerializeToString,
            mathdb__pb2.BinaryOpResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Mult(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Mult',
            mathdb__pb2.BinaryOpRequest.SerializeToString,
            mathdb__pb2.BinaryOpResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Div(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/MathDb/Div',
            mathdb__pb2.BinaryOpRequest.SerializeToString,
            mathdb__pb2.BinaryOpResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
