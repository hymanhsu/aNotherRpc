#!/usr/bin/python

'''
channel.py - Socket implementation of Google's Protocol Buffers RPC
service interface.

This package contains classes providing a socket implementation of the
RpcChannel abstract class.

Not Thread Safe!

Currently, one channel use one connection

TODO: Use Load Balancer to send & receive

'''

# Standard library imports
import logging
import socket
import struct
import random

# Third party imports
import google.protobuf.service as service

# Module imports
from nrpc import controller
from nrpc import nrpc_pb2
from nrpc import errno_pb2
from nrpc import error
from nrpc import naming

logger = logging.getLogger('')
PROTOCOL_HEADER_LEN = 12

class SocketFactory():
    '''A factory class for providing socket instances.'''

    def createSocket(self):
        '''Creates and returns a TCP socket.'''
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


class SocketRpcChannel(service.RpcChannel):
    '''Socket implementation of an RpcChannel.

    An RpcChannel represents a communication line to a service which
    can be used to call the service's methods.

    Example:
       channel    = SocketRpcChannel(host='myservicehost')
       controller = channel.newController()
       service    = MyService_Stub(channel)
       service.MyMethod(controller,request,callback)
    '''

    def __init__(self, service_fullname=None, node_tags=None, 
                etcd_ip=None, etcd_port=2379, recv_buf=4096, retry_max=1,
                socketFactory=SocketFactory()):
        '''SocketRpcChannel to connect to a socket server
        on a user defined port.
        '''
        self.serviceFullName = service_fullname
        self.retryMax = retry_max
        self.recvBuf = recv_buf
        self.serviceFinder = naming.ServiceFinder(service_fullname,node_tags,etcd_ip,etcd_port)
        self.sockFactory = socketFactory
        self.sock = None


    def newController(self):
        '''Create and return a socket controller.'''
        return controller.SocketRpcController()


    def validateRequest(self, request):
        '''Validate the client request against the protocol file.'''

        # Check the request is correctly initialized
        if not request.IsInitialized():
            raise error.BadRequestProtoError('Client request is missing\
                                              mandatory fields')


    def findSocket(self):
        '''Open a socket connection to a given host and port.'''

        # Return the socket if exist
        if self.sock is not None:
            return self.sock

        # Find right peer endpoint from naming service
        endpoints = self.serviceFinder.getEndpoints()
        if len(endpoints) == 0:
            raise error.ServiceNotFoundError('Not found service {} from naming'.format(self.serviceFullName))

        random.seed()
        index = random.randint(0, len(endpoints)-1)
        endpoint = endpoints[index]
        host = endpoint[0]
        port = endpoint[1]
        logger.debug( 'Found endpoint {}:{} of {}'.format(host, str(port), self.serviceFullName) )

        self.sock = self.sockFactory.createSocket()

        # Connect socket to server - defined by host and port arguments
        try:
            self.sock.connect((host, port))
            logger.debug( 'connect to {}:{} successfully'.format(host,str(port)) )
        except socket.gaierror:
            msg = "Could not find host %s" % host
            # Cleanup and re-raise the exception with the caller
            self.closeSocket(self.sock)
            raise error.UnknownHostError(msg)
        except socket.error:
            msg = "Could not open I/O for %s:%s" % (host, port)
            # Cleanup and re-raise the exception with the caller
            self.closeSocket(self.sock)
            raise error.IOError(msg)

        return self.sock


    def createRpcRequest(self, method, request):
        '''Wrap the user's request in an RPC protocol message.'''
        nrpcMeta = nrpc_pb2.NrpcMeta()
        nrpcMeta.correlation_id = 0
        nrpcRequestMeta = nrpcMeta.request
        nrpcRequestMeta.service_name = method.containing_service.full_name
        nrpcRequestMeta.method_name = method.name
        nrpcRequestMeta.request_body = request.SerializeToString()
        nrpcRequestMeta.log_id = 0
        nrpcRequestMeta.trace_id = 0
        nrpcRequestMeta.span_id = 0
        nrpcRequestMeta.parent_span_id = 0

        return nrpcMeta


    def sendRpcMessage(self, sock, rpcRequest):
        '''Send an RPC request to the server.'''
        try:
            body = rpcRequest.SerializeToString()
            total_len = len(body) + PROTOCOL_HEADER_LEN
            msg_flag = 0
            data = b''.join([struct.pack('!4si4b', b'NRPC', total_len,msg_flag,0,0,0), body])
            #sock.send(data[0:10])
            #sock.send(data[10:])
            sock.sendall(data)
            logger.debug('sendRpcMessage data len = {}'.format( str(len(data)) ))
        except (BrokenPipeError, IOError):
            self.closeSocket(sock)
            raise error.IOError("Error writing data to server")
        except socket.error:
            self.closeSocket(sock)
            raise error.IOError("Error writing data to server")

            
    def recvRpcMessage(self, sock):
        '''Handle reading an RPC reply from the server.'''
        try:
            # Read all data into byte_stream
            byte_stream = sock.recv(self.recvBuf)
            logger.debug('recvRpcMessage data len = {}'.format( str(len(byte_stream)) ))
        except (BrokenPipeError, IOError):
            self.closeSocket(sock)
            raise error.IOError("Error reading data from server")
        except socket.error:
            self.closeSocket(sock)
            raise error.IOError("Error reading data from server")
            
        tag_,total_len,msg_flag,_,_,_  = struct.unpack('!4si4b', byte_stream[0:PROTOCOL_HEADER_LEN])
        tag = tag_.decode("utf-8")
        if tag != 'NRPC':
            self.closeSocket(sock)
            raise error.BadResponseProtoError('Read wrong tag from server : {}'.format(tag))  
        serialized = byte_stream[PROTOCOL_HEADER_LEN:]
        if total_len != len(serialized) + PROTOCOL_HEADER_LEN:
            self.closeSocket(sock)
            raise error.BadResponseProtoError("Read wrong data length from server")

        if msg_flag != 1:
            self.closeSocket(sock)
            raise error.BadResponseProtoError("Read wrong msg type from server")  

        return serialized


    def parseResponse(self, byte_stream, response_class):
        '''Parse a bytestream into a Response object of the requested type.'''

        # Instantiate a Response object of the requested type
        response = response_class()

        # Catch anything which isn't a valid PB bytestream
        try:
            response.ParseFromString(byte_stream)
        except Exception as e:
            raise error.BadResponseProtoError("Invalid response \
                                              (decodeError): " + str(e))

        # Check the response has all mandatory fields initialized
        if not response.IsInitialized():
            raise error.BadResponseProtoError("Response not initialized")

        return response


    def closeSocket(self, sock):
        '''Close the socket.'''
        if sock:
            try:
                sock.close()
            except:
                pass
        self.sock = None
        return


    def tryToRunCallback(self, done):
        # Check for any outstanding errors
        #TODO

        # If blocking, return response or raise error
        if done is None:
            if self.rpcResponse.response.error_code != 0:
                raise error.RpcError(self.rpcResponse.response.error_text)
            else:
                return self.serviceResponse

        # Run the callback
        done.run(self.serviceResponse)
        

    def CallMethod(self, method, controller, request, response_class, done):
        '''Call the RPC method.
        '''
        retry = self.retryMax + 1
        while retry > 0:
            retry -= 1
            controller.reset()
            try:
                # Find one avaliable socket
                sock = self.findSocket()
                # Create an RPC request protobuf
                rpcRequest = self.createRpcRequest(method, request)
                self.sendRpcMessage(sock, rpcRequest)
                self.reply = self.recvRpcMessage(sock)
                self.rpcResponse = self.parseResponse(self.reply,nrpc_pb2.NrpcMeta)
                if self.rpcResponse.response.error_code != 0:
                    controller.handleError(self.rpcResponse.response.error_code,self.rpcResponse.response.error_text)
                    logger.error('CallMethod error : {}'.format( self.rpcResponse.response.error_text ))
                    return

                self.serviceResponse =  self.parseResponse(self.rpcResponse.response.response_body,response_class)
                return self.tryToRunCallback(done)
            except error.ProtobufError as e:
                controller.handleError(e.rpc_error_code,e.message)
                logger.error('CallMethod error : {0}'.format(e))
            except Exception as e:
                controller.handleError(errno_pb2.SYS_EREMOTEIO,'{0}'.format(e))
                logger.error('CallMethod error : {0}'.format(e))

