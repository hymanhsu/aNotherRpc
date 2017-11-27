import os, sys, time
sys.path.append(os.path.abspath('../../'))
sys.path.append(os.path.abspath('./'))

import logging
from nrpc import SocketRpcChannel,SocketRpcController,NrpcServer
from echo_pb2 import EchoService,EchoRequest,EchoResponse
from nrpc.server import startThreadPool,stopThreadPool,registerService
import echo_service_imply
import gflags

FLAGS = gflags.FLAGS
gflags.DEFINE_string('etcd_ip', '127.0.0.1', 'Etcd server IP')
gflags.DEFINE_string('node_ip', '192.168.122.128', 'Node server IP')
gflags.DEFINE_integer('port', 8000, 'Listenning port')  
FLAGS(sys.argv)

logger = logging.getLogger('')
fomatter = logging.Formatter('%(asctime)s %(threadName)15s %(levelname)8s %(message)s (%(filename)s:%(lineno)s)')
#输出到屏幕
console = logging.StreamHandler(sys.stdout)
console.setFormatter(fomatter)
console.flush = sys.stdout.flush
#输出到文件
#fh = logging.FileHandler("log2.log")
#fh.setLevel(logging.INFO)
#fh.setFormatter(fomatter)
#设置日志格式
logger.addHandler(console)
logger.setLevel(logging.DEBUG)
#logger.addHandler(fh)


listenPort = FLAGS.port

#service impl
service = echo_service_imply.EchoServiceImpl()
registerService(service)

#启动线程池
startThreadPool(3)

#启动服务器
server = NrpcServer('0.0.0.0',
    port=listenPort,
    node_ip=FLAGS.node_ip,
    node_tags='stage=beta;version=1.0',
    etcd_ip=FLAGS.etcd_ip,
    etcd_port=2379,
    check_interval_secs=5
)
server.start()

#destroy
server.stop()
stopThreadPool()
