import logging
import echo_pb2

from nrpc import service

logger = logging.getLogger('')

class EchoServiceImpl(echo_pb2.EchoService,service.BaseService):
    
    def checkValid(self):
        return True

    def echo(self, controller, request, done):
        message = request.message
        
        logger.debug('EchoServiceImpl received data : ' + message)
        response = echo_pb2.EchoResponse()
        response.message = message

        return response
