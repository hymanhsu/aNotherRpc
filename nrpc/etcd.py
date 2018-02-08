#!/usr/bin/python

'''
etcd.py - Etcd client
'''
import logging
import os 
import json
import urllib
from urllib.request import urlopen

logger = logging.getLogger('')


class EtcdOpsException(Exception):
    
    def __init__(self, status, content):
        msg = json.dumps({'status': status, 'content': content})
        super(EtcdOpsException, self).__init__(msg)


def parse_etcd_response(data):
    result = {}
    key = ''
    if 'key' in data:
        key = data['key']

    if 'dir' in data and data['dir']:
        if 'nodes' in data:
            for node in data['nodes']:
                if 'dir' in node and node['dir']:
                    result.update(parse_etcd_response(node))
                else:
                    result[node['key']] = node['value']
    else:
        result[key] = data['value']

    return result


class EtcdHTTPConnect:

    def request(self, method_, uri, params=None):
        try:
            if params is None:
                request = urllib.request.Request(uri, method=method_)
            else:
                request = urllib.request.Request(uri, data=urllib.parse.urlencode(params).encode('utf-8'), 
                    method=method_)
            response = urllib.request.urlopen(request, timeout=3)
            return (response.status, response.read().decode('utf-8'))
        except:
            logger.error('Failed to write data to etcd!')
            return (500, '')

    
class EtcdClient:
    
    def __init__(self, host='127.0.0.1', port=2379,
            protocol='http', conn_cls=EtcdHTTPConnect, **kwargs):

        self.protocol = protocol
        self.host = host
        self.port = port
        self.conn = conn_cls(**kwargs)


    def get_uri(self, key):
        if key.startswith('/'):
            key = key[1:]
        return '%s://%s:%s/v2/keys/%s' % (self.protocol, self.host, self.port, key)


    def get(self, key, raw=False):
        uri = self.get_uri(key)+'?recursive=true'
        status, content = self.conn.request('GET', uri)

        if status == 200:
            logger.debug(content)
            data = json.loads(content)
            if raw:
                return data

            node = data['node']
            result = parse_etcd_response(node)
            #for k,v in result.items():
            #    logger.debug(k)

            return result

        raise EtcdOpsException(status, content)


    def delete(self, key):
        uri  = self.get_uri(key)
        status, content = self.conn.request('DELETE', uri+"?recursive=true")
        if status == 200:
            return True
        raise EtcdOpsException(status, content)


    def create(self, key, data, ttl):
        if type(data) in (str, bytes, int, float):
            params = {
                "value": data, 
                "ttl" : ttl
                }
            status, content = self.conn.request('PUT', self.get_uri(key), params)
            if status == 200 or status == 201:
                return True
            raise EtcdOpsException(status, content)


    def update(self, key, data, ttl):
        if type(data) in (str, bytes, int, float):
            params = {
                "value": data, 
                "ttl" : ttl,
                "prevExist": True
                }
            status, content = self.conn.request('PUT', self.get_uri(key), params)
            if status == 200 or status == 201:
                return True
            raise EtcdOpsException(status, content)


