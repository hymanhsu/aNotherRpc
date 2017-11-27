import os, sys, time
sys.path.append(os.path.abspath('../../'))
sys.path.append(os.path.abspath('./'))

import logging
from nrpc import SocketRpcChannel
from echo_pb2 import EchoService_Stub,EchoRequest,EchoResponse
import gflags

FLAGS = gflags.FLAGS
gflags.DEFINE_string('etcd_ip', '127.0.0.1', 'Etcd server IP')  
gflags.DEFINE_integer('port', 8000, 'Listenning port')  
FLAGS(sys.argv)

logger = logging.getLogger('')
fomatter = logging.Formatter('%(asctime)s %(threadName)15s %(levelname)8s %(message)s (%(filename)s:%(lineno)s)')
#输出到屏幕
console = logging.StreamHandler()
console.setFormatter(fomatter)
#输出到文件
#fh = logging.FileHandler("log2.log")
#fh.setLevel(logging.INFO)
#fh.setFormatter(fomatter)
#设置日志格式
logger.addHandler(console)
logger.setLevel(logging.DEBUG)
#logger.addHandler(fh)


port = FLAGS.port

# Create a request
request = EchoRequest()
request.message = 'Hello world'

channel    = SocketRpcChannel(
    service_fullname='sogou.nlu.rpc.example.EchoService',
    node_tags='stage=beta;version=1.0',
    etcd_ip=FLAGS.etcd_ip,
    etcd_port=2379
)
controller = channel.newController()
service    = EchoService_Stub(channel)

start_time_milisec = int(round(time.time() * 1000))
successCount = 0
count = 50
while count > 0:
    count -= 1
    print(count)
    response = service.echo(controller,request,None)
    if controller.failed():
        continue
    logger.debug(response.message)
    successCount += 1
    time.sleep(.500)

end_time_milisec = int(round(time.time() * 1000))
cost_time_milisec = end_time_milisec - start_time_milisec
logger.debug( 'cost time mili seconds : {}'.format( str(cost_time_milisec) ) )
logger.debug( 'success count : {}'.format( str(successCount) ) )
