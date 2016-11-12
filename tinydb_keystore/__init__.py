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

    def keyrestore(self, keystore_dict, modify_dict):
        """
        Restore the keys in the database element recursively.
        """
        for itemid, itemval in modify_dict.items():
            if itemid in keystore_dict.values():
                match_value = list(keystore_dict.keys())[list(keystore_dict.values()).index(itemid)]
                modify_dict[match_value] = modify_dict.pop(itemid)
            
            if type(itemval) == dict:
                self.keyrestore(keystore_dict, itemval)

    def keyreplace(self, keystore_dict, modify_dict):
        """
        Replace the keys in the database element recursively. If the ID exists in the 'keystore' then
        replace the ID with the keystore translation, otherwise replace it with an item from the keylist
        and add the new translation to the keystore
        """
        for itemid, itemval in modify_dict.items():
            # Check whether the itemid can be replaced. Populate they keystore dict with new relationship
            # i.e There are keys in the keylist, the itemname isn't already in the keystore, and the itemname wasn't already replaced.
            if ( len(self.keylist) > 0 ) and ( itemid not in keystore_dict ) and ( itemid not in keystore_dict.values() ):
                keystore_dict[itemid] = self.keylist.pop(0)
                
            # Do the actual replace
            if itemid in keystore_dict:
                modify_dict[keystore_dict[itemid]] = modify_dict.pop(itemid)
                
            # Do the same process recursively for all dicts
            if type(itemval) == dict:
                self.keyreplace(keystore_dict, itemval)
            
        
    def read(self):
        data = self.storage.read()
        if data is None:
            return data

        if 'keystore' not in data:
            data['keystore'] = {}
        
        for tableid, tableval in data.items():
            if tableid != 'keystore':
                for elid, elval in tableval.items():
                    self.keyrestore(data['keystore'], elval)
        
        return data

    def write(self, data):
        """
        TODO: Create the keystore as a TinyDB Table instead of a dict
        """
        if 'keystore' not in data:
            data['keystore'] = {}
        
        for tableid, tableval in data.items():
            if tableid != 'keystore':
                for elid, elval in tableval.items():
                    self.keyreplace(data['keystore'], elval)
        
        self.storage.write(data)
