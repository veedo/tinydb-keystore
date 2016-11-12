# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 20:39:36 2016

@author: Zander Erasmus
"""
from tinydb import TinyDB
from tinydb.middlewares import Middleware
from collections import OrderedDict

class KeystoreMiddleware(Middleware):
    """
    Add a keystore table to reduce storage space
    Mainly intended for binary storage methods that allow raw types of 
    key storage rather than strings

    Could be used eventually in some hashmapping style optomization
    """

    keylist = []

    def __init__(self, storage_cls=TinyDB.DEFAULT_STORAGE, key_list=[]):
        super(KeystoreMiddleware, self).__init__(storage_cls)
        self.keylist = list(OrderedDict.fromkeys(key_list))

    def read(self):
        data = self.storage.read()
        if data is None:
            return data

        """
        Do some cool stuff here
        """
        if 'keystore' not in data:
            data['keystore'] = {}
        
        
        
        return data

    def write(self, data):
        """
        Do more cool stuff here
        """
        if 'keystore' not in data:
            data['keystore'] = {}
        
        
        
        self.storage.write(data)
