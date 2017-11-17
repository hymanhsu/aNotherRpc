#!/usr/bin/python

'''
server.py - Implementation of Nlu Server

Protocol Desciption:


4 bytes('NRPC') | 4 bytes(Total Length) | 1 byte(0意味请求,1意味应答) | 1 byte(Reserved) | 1 byte(Reserved) | 1 byte(Reserved) | payload .... 


'''

# Standard library imports
import logging
import asyncio
import threading
import struct
import traceback
import sys
import json
import base64
import time

# Third-party imports


# Module imports
from nrpc import controller
from nrpc import thread_pool
from nrpc import thread_job
from nrpc import nrpc_pb2
from nrpc import error
from nrpc import etcd

logger = logging.getLogger('')

# global variable
PROTOCOL_HEADER_LEN = 12
pool = thread_pool.ThreadPool()
serviceMap = {}


class ServiceCheckThread(threading.Thread):
    '''Check user's services' status'''

    def __init__(self):
        threading.Thread.__init__(self)


    def init(self, port=8000, node_ip=None, node_tags=None, etcd_ip=None, etcd_port=2379, check_interval_secs=5):
        self.nodeIP = node_ip
        self.nodePort = port
        self.nodeTags = node_tags
        self.checkIntervalSecs = check_interval_secs
        self.etcdIP = etcd_ip
        self.etcdPort = etcd_port
        self.running = True
        

    def stop(self):
        self.running = False


    def run(self):
        logger.info('Starting ServiceCheckThread')

        firstTime = True

        #TODO : check service's status
        while self.running:
            
            registerContent = {}

            nodeInfo = {}
            nodeInfo['ip'] = self.nodeIP
            nodeInfo['port'] = self.nodePort
            registerContent['node'] = nodeInfo

            tagsInfo = {}
            if self.nodeTags is not None and self.nodeTags != '':
                pairList = self.nodeTags.split(';')
                for pair in pairList:
                    valueList = pair.split('=')
                    if len(valueList) == 2:
                        tagsInfo[valueList[0]] = valueList[1]
            registerContent['tags'] = tagsInfo

            serviceNames = []
            for k,v in serviceMap.items():
                serviceFullName = k
                service = v
                if service is not None:
                    if service.checkValid():
                        serviceNames.append(serviceFullName)
            registerContent['services'] = serviceNames

            register_str = json.dumps(registerContent)
            logger.debug(register_str)

            # save register info into etcd server
            register_encoded = base64.b64encode(register_str.encode('ascii'))
            key = '/providers/' + str(register_encoded,encoding = "utf-8")
            value = ''
            ttl = self.checkIntervalSecs + 2

            try:
                etcdClient = etcd.EtcdClient(self.etcdIP,self.etcdPort)
                if firstTime:
                    etcdClient.create(key,value,ttl)
                    firstTime = False
                else:
                    etcdClient.update(key,value,ttl)
            except etcd.EtcdOpsException as e:
                logger.error("Etcd access error: {0}".format(e))

            #sleep
            time.sleep(self.checkIntervalSecs)

        logger.info('End ServiceCheckThread')


class Callback():
    '''Class to allow execution of client-supplied callbacks.'''

    def __init__(self):
        self.invoked = False
        self.response = None

    def run(self, response):
        self.response = response
        self.invoked = True


def startThreadPool(threadNum=6):
    pool.set_thread_num(threadNum)
    pool.start()


def stopThreadPool():
    pool.finish()


def registerService(service):
    '''Register an RPC service.'''
    serviceMap[service.GetDescriptor().full_name] = service


def parseServiceRequest(bytestream_from_client):
    '''Validate the data stream received from the client.'''

    #print 'got request : ' + str(len(bytestream_from_client))

    # Convert the client request into a PB Request object
    nrpcMeta = nrpc_pb2.NrpcMeta()

    # Catch anything which isn't a valid PB bytestream
    try:
        nrpcMeta.MergeFromString(bytestream_from_client)
    except Exception as e:
        raise error.BadRequestDataError("Invalid request from \
                                            client (decodeError): " + str(e))

    # Check the request is correctly initialized
    if not nrpcMeta.IsInitialized():
        raise error.BadRequestDataError("Client request is missing \
                                             mandatory fields")

    return nrpcMeta


def retrieveService(service_name):
    '''Match the service request to a registered service.'''
    service = serviceMap.get(service_name)
    if service is None:
        msg = "Could not find service '%s'" % service_name
        raise error.ServiceNotFoundError(msg)

    return service


def retrieveMethod(service, method_name):
    '''Match the method request to a method of a registered service.'''
    method = service.DESCRIPTOR.FindMethodByName(method_name)
    if method is None:
        msg = "Could not find method '%s' in service '%s'"\
                   % (method_name, service.DESCRIPTOR.name)
        raise error.MethodNotFoundError(msg)

    return method


def retrieveProtoRequest(service, method, nrpcMeta):
    ''' Retrieve the users protocol message from the RPC message'''
    proto_request = service.GetRequestClass(method)()
    
    try:
        proto_request.ParseFromString(nrpcMeta.request.request_body)
    except Exception as e:
        raise error.BadRequestProtoError(unicode(e))

    # Check the request parsed correctly
    if not proto_request.IsInitialized():
        raise error.BadRequestProtoError('Invalid protocol request \
                                              from client')

    return proto_request


def callServiceMethod(service, method, proto_request, nrpcMeta):
    '''Execute a service method request.'''

    # Create the controller (initialised to success) and callback
    controllerInstance = controller.SocketRpcController()
    callback = Callback()
    try:
        response_proto = service.CallMethod(method, controllerInstance, proto_request, callback)
    except Exception as e:
        raise error.RpcError(unicode(e))

    # Return an RPC response, with payload defined in the callback
    respNrpcMeta = nrpc_pb2.NrpcMeta()
    respNrpcMeta.correlation_id = nrpcMeta.correlation_id
    nrpcResponseMeta = respNrpcMeta.response
    nrpcResponseMeta.error_code = 0
    nrpcResponseMeta.error_text = ''
    nrpcResponseMeta.response_body = response_proto.SerializeToString()

    # Check to see if controller has been set to not success by user.
    if controllerInstance.failed():
        nrpcResponseMeta.error_text = controllerInstance.error()
        nrpcResponseMeta.error_code = controllerInstance.errorCode()

    return respNrpcMeta


def handleError(e):
    '''Produce an RPC response to convey a server error to the client.'''
    msg = "%d : %s" % (e.rpc_error_code, e.message)
    logger.error(msg)

    # Create error reply
    response = nrpc_pb2.Response()
    response.error_code = e.rpc_error_code
    response.error = e.message
    return response


def executeRequest(nrpcMeta):
    '''Match a client request to the corresponding service and method on
    the server, and then call the service.'''

    # Retrieve the requested service
    try:
        service = retrieveService(nrpcMeta.request.service_name)
        #print('service_name : '+request.service_name)
    except error.ServiceNotFoundError as e:
        return handleError(e)

    # Retrieve the requested method
    try:
        method = retrieveMethod(service, nrpcMeta.request.method_name)
        #print('method_name : '+request.method_name)
    except error.MethodNotFoundError as e:
        return handleError(e)

    # Retrieve the protocol message
    try:
        proto_request = retrieveProtoRequest(service, method, nrpcMeta)
        #print('retrieveProtoRequest ')
    except error.BadRequestProtoError as e:
        return handleError(e)

    # Execute the specified method of the service with the requested params
    try:
        response = callServiceMethod(service, method, proto_request, nrpcMeta)
        #print('callMethod ')
    except error.RpcError as e:
        return handleError(e)

    return response


def workerCallback(*args,**kwargs):
    #print('workerCallback ')
    curThdName = threading.current_thread().name
    transport = args[0]
    nrpcMeta = args[1]

    # execute service
    rpcResponse = executeRequest(nrpcMeta)
    #print('executeRequest ')
    response_body = rpcResponse.SerializeToString()
    total_len = len(response_body) + PROTOCOL_HEADER_LEN
    msg_flag = 1
    data = b''.join([struct.pack('!4si4b', b'NRPC',total_len,msg_flag,0,0,0), response_body])

    #send response
    try:
        transport.write(data)
        logger.debug('Send response len = {}'.format( str(len(data)) ) )
        #print('{} : sent done'.format(curThdName))
    except socket.error as e:
        logger.error('Socket error : {0}'.format(e))


class NrpcProtocol(asyncio.Protocol):
    
    def __init__(self):
        '''NrpcProtocol to handle receive data
        '''
        self.clientReadBuf = bytes()


    def connection_made(self, transport):
        self.peername = transport.get_extra_info('peername')
        self.socket = transport.get_extra_info('socket')
        self.fileno = self.socket.fileno()
        logger.info('Connection from {}'.format(self.peername))
        self.transport = transport


    def data_received(self, data):
        #use mix-buffer to read data
        self.clientReadBuf = self.clientReadBuf + bytes(data)

        while True:

            bufLen = len(self.clientReadBuf)
            if bufLen < PROTOCOL_HEADER_LEN:
                logger.warning('Header need 12 bytes')
                break
            
            tag_,total_len,msg_flag,_,_,_ = struct.unpack('!4si4b', self.clientReadBuf[0:PROTOCOL_HEADER_LEN])
            tag = tag_.decode("utf-8")
            if tag != 'NRPC':
                self.transport.close()
                logger.error('Request tag error : {}'.format(tag))
                break
            if msg_flag != 0:
                logger.error('Request msg type error')
                self.transport.close()
                break

            if bufLen < total_len:
                logger.warning('Data field need more bytes')
                break

            serialized = self.clientReadBuf[PROTOCOL_HEADER_LEN:total_len]
            logger.debug('Receive request len = {}'.format( str(total_len) ) )

            # Parse and validate the client's request
            try:
                request = parseServiceRequest(serialized)
            except error.BadRequestDataError as e:
                logger.error('request decode error')
                self.transport.close()
                s=traceback.format_exc()
                logger.error(s)
                break

            job = thread_job.ThreadJob(workerCallback, (self.transport,request), {})
            pool.add_job(job)
            self.clientReadBuf = self.clientReadBuf[total_len:]

        

    def eof_received(self):
        logger.info('Read eof from {}'.format(self.peername))

        
    def connection_lost(self, exc):
        logger.info('Peer closed from {}'.format(self.peername))


class NrpcServer:

    def __init__(self, host='0.0.0.0', port=8000, node_ip=None, node_tags=None, etcd_ip=None, etcd_port=2379, check_interval_secs=5 ):
        '''port - Port this server is started on'''
        self.port = port
        self.host = host
        self.loop = asyncio.get_event_loop()
        self.coro = self.loop.create_server(NrpcProtocol, self.host, self.port)
        self.addons = {}
        self.addons['port'] = self.port
        self.addons['node_ip'] = node_ip
        self.addons['node_tags'] = node_tags
        self.addons['etcd_ip'] = etcd_ip
        self.addons['etcd_port'] = etcd_port
        self.addons['check_interval_secs'] = check_interval_secs
        try:
            self.serviceCheckThread = ServiceCheckThread()
            self.serviceCheckThread.init(**self.addons)
        except Exception:
            s=traceback.format_exc()
            logger.error(s)


    def startServiceCheck(self):
        self.serviceCheckThread.start()


    def stopServiceCheck(self):
        if self.serviceCheckThread is not None:
            self.serviceCheckThread.stop()

    
    def start(self):
        self.server = self.loop.run_until_complete(self.coro)
        logger.info('Serving on {}'.format(self.server.sockets[0].getsockname()))
        self.startServiceCheck()
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass


    def stop(self):
        self.stopServiceCheck()
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

