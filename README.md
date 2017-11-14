# 介绍

aNother Remote Process Call Framwork

基于Protobuf3.2版本搭建的RPC框架，自带服务注册与发现功能，具备基本的服务框架的能力。

Support python 3.x


# 安装

```
python setup.py install
```


# 用法

## 服务器端
```python
service = echo_service_imply.EchoServiceImpl()
registerService(service)
startThreadPool(3)
server = NrpcServer('0.0.0.0',
    port=listenPort,
    node_ip='192.168.122.128',
    node_tags='stage=beta;version=1.0',
    etcd_ip='127.0.0.1',
    etcd_port=2379,
    check_interval_secs=5
)
server.start()

```

## 客户端
```python
request = EchoRequest()
request.message = 'Hello world'
channel    = SocketRpcChannel(
    service_fullname='sogou.nlu.rpc.example.EchoService',
    node_tags='stage=beta;version=1.0',
    etcd_ip='127.0.0.1',
    etcd_port=2379
)
controller = channel.newController()
service    = EchoService_Stub(channel)
response = service.echo(controller,request,None)
if not controller.failed():
    logger.debug(response.message)


```

