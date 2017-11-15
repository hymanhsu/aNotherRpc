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
from nrpc import error
from nrpc import naming

logger = logging.getLogger('')
PROTOCOL_HEADER_LEN = 8

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
        rpcRequest = nrpc_pb2.Request()
        rpcRequest.request_proto = request.SerializeToString()
        rpcRequest.service_name = method.containing_service.full_name
        rpcRequest.method_name = method.name

        return rpcRequest

    def sendRpcMessage(self, sock, rpcRequest):
        '''Send an RPC request to the server.'''
        try:
            body = rpcRequest.SerializeToString()
            total_len = len(body) + PROTOCOL_HEADER_LEN
            msg_flag = 0
            data = b''.join([struct.pack('!i4b', total_len,msg_flag,0,0,0), body])
            #sock.send(data[0:10])
            #sock.send(data[10:])
            sock.sendall(data)
            logger.debug('sendRpcMessage data len = {}'.format( str(len(data)) ))
        except socket.error:
            self.closeSocket(sock)
            raise error.IOError("Error writing data to server")


    def recvRpcMessage(self, sock):
        '''Handle reading an RPC reply from the server.'''
        try:
            # Read all data into byte_stream
            byte_stream = sock.recv(self.recvBuf)
            logger.debug('recvRpcMessage data len = {}'.format( str(len(byte_stream)) ))
        except socket.error:
            self.closeSocket(sock)
            raise error.IOError("Error reading data from server")            
        
        total_len,msg_flag,_,_,_  = struct.unpack('!i4b', byte_stream[0:PROTOCOL_HEADER_LEN])
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
            if self.rpcResponse.error_code != nrpc_pb2.NOERROR:
                raise error.RpcError(self.rpcResponse.error)
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
                self.rpcResponse = self.parseResponse(self.reply,nrpc_pb2.Response)
                self.serviceResponse =  self.parseResponse(self.rpcResponse.response_proto,response_class)
                return self.tryToRunCallback(done)
            except Exception as e:
                controller.handleError(nrpc_pb2.IO_ERROR,'{0}'.format(e))
                logger.error('CallMethod error : {0}'.format(e))


"""
    def CallMethod(self, method, controller, request, response_class, done):
        '''Call the RPC method.

        This method uses a LifeCycle instance to manage communication
        with the server.
        '''
        lifecycle = _LifeCycle(controller, self)
        lifecycle.tryToValidateRequest(request)
        lifecycle.tryToOpenSocket()
        lifecycle.tryToSendRpcRequest(method, request)
        lifecycle.tryToReceiveReply()
        lifecycle.tryToParseReply()
        lifecycle.tryToRetrieveServiceResponse(response_class)
        return lifecycle.tryToRunCallback(done)

"""

class _LifeCycle():
    '''Represents and manages the lifecycle of an RPC request.'''

    def __init__(self, controller, channel):
        self.controller = controller
        self.channel = channel
        self.sock = None
        self.byte_stream = None
        self.rpcResponse = None
        self.serviceResponse = None

    def tryToValidateRequest(self, request):
        if self.controller.failed():
            return

        # Validate the request object
        try:
            self.channel.validateRequest(request)
        except error.BadRequestProtoError as e:
            self.controller.handleError(nrpc_pb2.BAD_REQUEST_PROTO,
                                        e.message)

    def tryToOpenSocket(self):
        if self.controller.failed():
            return

        # Open socket
        try:
            self.sock = self.channel.openSocket(self.channel.host,
                                                self.channel.port)
        except error.UnknownHostError as e:
            self.controller.handleError(nrpc_pb2.UNKNOWN_HOST,
                                        e.message)
        except error.IOError as e:
            self.controller.handleError(nrpc_pb2.IO_ERROR, e.message)

    def tryToSendRpcRequest(self, method, request):
        if self.controller.failed():
            return

        # Create an RPC request protobuf
        rpcRequest = self.channel.createRpcRequest(method, request)

        # Send the request over the socket
        try:
            self.channel.sendRpcMessage(self.sock, rpcRequest)
        except error.IOError as e:
            self.controller.handleError(nrpc_pb2.IO_ERROR, e.message)

    def tryToReceiveReply(self):
        if self.controller.failed():
            return

        # Get the reply
        try:
            self.byte_stream = self.channel.recvRpcMessage(self.sock)
        except error.IOError as e:
            self.controller.handleError(nrpc_pb2.IO_ERROR, e.message)

    def tryToParseReply(self):
        if self.controller.failed():
            return

        #Parse RPC reply
        try:
            self.rpcResponse = self.channel.parseResponse(self.byte_stream,
                                                          nrpc_pb2.Response)
        except error.BadResponseProtoError as e:
            self.controller.handleError(nrpc_pb2.BAD_RESPONSE_PROTO, e.message)

    def tryToRetrieveServiceResponse(self, response_class):
        if self.controller.failed():
            return

        if self.rpcResponse.HasField('error'):
            self.controller.handleError(self.rpcResponse.error_reason,
                                        self.rpcResponse.error)
            return
        
        if self.rpcResponse.HasField('response_proto'):
            # Extract service response
            try:
                self.serviceResponse = self.channel.parseResponse(
                    self.rpcResponse.response_proto, response_class)
            except error.BadResponseProtoError as e:
                self.controller.handleError(nrpc_pb2.BAD_RESPONSE_PROTO,
                                            e.message)

    def tryToRunCallback(self, done):
        # Check for any outstanding errors
        if not self.controller.failed() and self.rpcResponse.error:
            self.controller.handleError(self.rpcResponse.error_reason,
                                        self.rpcResponse.error)

        # If blocking, return response or raise error
        if done is None:
            if self.controller.failed():
                raise error.RpcError(self.controller.error())
            else:
                return self.serviceResponse

        # Run the callback
        if self.controller.failed() or self.rpcResponse.callback:
            done.run(self.serviceResponse)
