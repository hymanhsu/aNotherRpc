#!/usr/bin/python

'''
naming.py - Simple naming service for finding user service's endpoint
'''

# Standard library imports
import logging
import threading
import traceback
import sys
import json
import base64
import time

# Module imports
from nrpc import error
from nrpc import etcd

logger = logging.getLogger('')


class ServiceFinder:
    
    def __init__(self, service_fullname=None, node_tags=None, etcd_ip=None, etcd_port=2379):
        self.serviceFullName = service_fullname
        self.nodeTags = node_tags
        self.etcdIP = etcd_ip
        self.etcdPort = etcd_port
        self.prefix = '/providers/'
        self.serviceTags = self.parseTags(self.nodeTags)


    def parseTags(self, content):
        tagsInfo = {}
        if content is not None and content != '':
            pairList = content.split(';')
            for pair in pairList:
                valueList = pair.split('=')
                if len(valueList) == 2:
                    tagsInfo[valueList[0]] = valueList[1]
        return tagsInfo


    def empty(self, tags=None):
        if tags is None or len(tags) == 0:
            return True
        return False


    def matchTags(self, nodeTags=None):
        if self.empty(self.serviceTags) and self.empty(nodeTags):
            return True
        if not self.empty(self.serviceTags) and self.empty(nodeTags):
            return False
        if self.empty(self.serviceTags) and not self.empty(nodeTags):
            return True
        # try to find serviceTags from nodeTags
        for key,value in self.serviceTags.items():
            value2 = nodeTags.get(key)
            if value2 is None or value2 != value:
                return False
        return True


    def matchServiceName(self, servicesList=None):
        if servicesList is None or len(servicesList) == 0:
            return False
        for item in servicesList:
            if item == self.serviceFullName:
                return True
        return False


    def getServiceName(self):
        return self.serviceFullName


    def getEndpoints(self):
        '''Query all matched endpoints'''
        result = []
        try:
            etcdClient = etcd.EtcdClient(self.etcdIP,self.etcdPort)
            valueResult = etcdClient.get('')
        except etcd.EtcdOpsException as e:
            logger.error("Etcd access error: {0}".format(e))

        if len(valueResult) > 0:
            for key,value in valueResult.items():
                if key.startswith(self.prefix):
                    # extract register content & base64 decode & json decode
                    registerContent = key[len(self.prefix):]
                    registerContentBytes = base64.b64decode(registerContent.encode('ascii'))
                    regcontent = str(registerContentBytes,encoding = "utf-8")
                    data = json.loads(regcontent)
                    # get main elements
                    nodeEle = data['node']
                    tagsEle = data['tags']
                    servicesEle = data['services']
                    # find matched node
                    nodeIP = nodeEle['ip']
                    nodePort = nodeEle['port']
                    if not self.matchTags(tagsEle):
                        # if tags not match , find next node
                        continue
                    if not self.matchServiceName(servicesEle):
                        # if services not match , find next node
                        continue
                    result.append( (nodeIP,nodePort) )

        return result

