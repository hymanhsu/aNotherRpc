#!/usr/bin/python

'''
service.py - Interface of service

'''

from abc import ABCMeta, abstractmethod

class BaseService:
    
    '''
    BaseService should be implemented by service impl

    '''
    @abstractmethod
    def checkValid(self):
        pass

