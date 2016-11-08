# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 20:39:36 2016

@author: Zander Erasmus
"""
from tinydb import TinyDB
from tinydb.middlewares import Middleware

class KeystoreMiddleware(Middleware):
    """
    Add a keystore table to reduce storage space
    Mainly intended for binary storage methods that allow raw types of 
    key storage rather than strings

    Could be used eventually in some hashmapping style optomization
    """

    def __init__(self, storage_cls=TinyDB.DEFAULT_STORAGE):
        super(KeystoreMiddleware, self).__init__(storage_cls)

    def read(self):
        data = self.storage.read()
        if data is None:
            return data

        """
        Do some cool stuff here
        """

        return data

    def write(self, data):
        """
        Do more cool stuff here
        """
        self.storage.write(data)
