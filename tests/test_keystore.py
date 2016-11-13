from tinydb import where
from tinydb.database import TinyDB
from tinydb_keystore import KeystoreMiddleware
from tinydb.storages import MemoryStorage
from conftest import db_middleware_empty_withkeylist, db_middleware_empty

import pytest

@pytest.mark.parametrize('db_', [db_middleware_empty(), db_middleware_empty_withkeylist()])
def test_newtable_addone(db_):
    """
    Test that adding a single element works in a new table
    """
    table = db_.table('someothertable')
    query1 = where('intvalue') == 1
    query2 = where('intvalue') == 2

    assert not table.search(query1)
    assert not table.search(query2)

    # Test insert
    table.insert({'intvalue': 1})

    assert len(table) == 1
    assert table.search(query1)
    assert not table.search(query2)

@pytest.mark.parametrize('db_', [db_middleware_empty(), db_middleware_empty_withkeylist()])
def test_newtable_addtwo_removefirst(db_):
    """
    Test that adds and removes elements
    """
    table = db_.table('someothertable')
    query1 = where('intvalue') == 1
    query2 = where('intvalue') == 2
    query3 = where('intvalue') == 3

    assert not table.search(query1)
    assert not table.search(query2)
    assert not table.search(query3)

    table.insert({'intvalue': 1})

    assert len(table) == 1
    assert table.search(query1)
    assert not table.search(query2)
    assert not table.search(query3)

    table.insert({'intvalue': 2})
    
    assert len(table) == 2
    assert table.search(query1)
    assert table.search(query2)
    assert not table.search(query3)

    table.remove(query1)

    assert len(table) == 1
    assert not table.search(query1)
    assert table.search(query2)
    assert not table.search(query3)

    table.remove(query2)

    assert len(table) == 0
    assert not table.search(query1)
    assert not table.search(query2)
    assert not table.search(query3)

def test_keylistinit_withelements():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c']))
    assert db_._storage.keylist == ['a', 'b', 'c']

def test_keylistinit_empty():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, []))
    assert db_._storage.keylist == []

def test_keylistinit_unprovided():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage))
    assert db_._storage.keylist == []

def test_keylistinit_duplicateselimated():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'b', 'c', 'd', 'e', 'd', 'b']))
    assert db_._storage.keylist == ['a', 'b', 'c', 'd', 'e']

def test_onelevelreplace():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd']))
    tb_ = db_.table()
    ks_tb_ = db_._storage.read()['keystore']
    
    tb_.insert({'myid1':1})

    assert len(tb_) == 1
    assert len(ks_tb_) == 1
    assert 'myid1' in ks_tb_.keys()
    assert ks_tb_['myid1'] == 'a'
    
    tb_.insert({'myid2':5})

    assert len(tb_) == 2
    assert len(ks_tb_) == 2
    assert 'myid2' in ks_tb_.keys()
    assert ks_tb_['myid2'] == 'b'

def test_twolevelreplace():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd']))
    tb_ = db_.table()
    ks_tb_ = db_._storage.read()['keystore']
    
    tb_.insert({'myid1':1, 'myid2':{'myid3':3}, 'myid4':4})
    
    assert len(tb_) == 1
    assert len(ks_tb_) == 4
    assert 'myid1' in ks_tb_.keys()
    assert 'myid2' in ks_tb_.keys()
    assert 'myid3' in ks_tb_.keys()
    assert 'myid4' in ks_tb_.keys()
    
    assert ks_tb_['myid1'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid2'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid3'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid4'] in ['a', 'b', 'c', 'd']
    
    old_ks_tb_ = ks_tb_.copy()
    tb_.insert({'myid4':{'myid2':2}})
    
    assert len(tb_) == 2
    assert len(ks_tb_) == 4
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert ks_tb_['myid3'] == old_ks_tb_['myid3']
    assert ks_tb_['myid4'] == old_ks_tb_['myid4']

def test_smallkeylist():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd']))
    tb_ = db_.table()
    ks_tb_ = db_._storage.read()['keystore']
    
    tb_.insert({'myid1':1, 'myid2':{'myid3':3}, 'myid4':4})
    
    assert len(tb_) == 1
    assert len(ks_tb_) == 4
    assert 'myid1' in ks_tb_.keys()
    assert 'myid2' in ks_tb_.keys()
    assert 'myid3' in ks_tb_.keys()
    assert 'myid4' in ks_tb_.keys()
    assert 'myid5' not in ks_tb_.keys()
    assert 'myid6' not in ks_tb_.keys()
    assert ks_tb_['myid1'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid2'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid3'] in ['a', 'b', 'c', 'd']
    assert ks_tb_['myid4'] in ['a', 'b', 'c', 'd']
    
    old_ks_tb_ = ks_tb_.copy()
    tb_.insert({'myid5':{'myid6':2}})
    
    assert len(tb_) == 2
    assert len(ks_tb_) == 4
    assert 'myid5' not in ks_tb_.keys()
    assert 'myid6' not in ks_tb_.keys()
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert ks_tb_['myid3'] == old_ks_tb_['myid3']
    assert ks_tb_['myid4'] == old_ks_tb_['myid4']

def test_mutipletable_add():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']))
    tb_ = [db_.table('tb1'), db_.table('tb2'), db_.table('tb3'), db_.table('tb4')]
    ks_tb_ = db_._storage.read()['keystore']
    
    assert set(db_.tables()) == set(['_default', 'keystore', 'tb1', 'tb2', 'tb3', 'tb4'])
    assert len(ks_tb_) == 0
    assert len(tb_[0]) == 0
    assert len(tb_[1]) == 0
    assert len(tb_[2]) == 0
    assert len(tb_[3]) == 0
    
    tb_[0].insert({'myid1':1})
    
    assert len(ks_tb_) == 1
    assert len(tb_[0]) == 1
    assert len(tb_[1]) == 0
    assert len(tb_[2]) == 0
    assert len(tb_[3]) == 0
    assert 'myid1' in ks_tb_.keys()
    assert ks_tb_['myid1'] == 'a'
    old_ks_tb_ = ks_tb_.copy()
    
    tb_[1].insert({'myid1':{'myid2':'someothervalue'}})
    
    assert len(ks_tb_) == 2
    assert len(tb_[0]) == 1
    assert len(tb_[1]) == 1
    assert len(tb_[2]) == 0
    assert len(tb_[3]) == 0
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert 'myid2' in ks_tb_.keys()
    assert ks_tb_['myid2'] == 'b'
    old_ks_tb_ = ks_tb_.copy()
    
    tb_[2].insert({'myid3':{'myid4':'c'}})
    
    assert len(ks_tb_) == 4
    assert len(tb_[0]) == 1
    assert len(tb_[1]) == 1
    assert len(tb_[2]) == 1
    assert len(tb_[3]) == 0
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert 'myid3' in ks_tb_.keys()
    assert 'myid4' in ks_tb_.keys()
    assert ks_tb_['myid3'] in ['c', 'd']
    assert ks_tb_['myid4'] in ['c', 'd']
    old_ks_tb_ = ks_tb_.copy()
    
    tb_[3].insert({'myid2':{'myid5':5}})
    
    assert len(ks_tb_) == 5
    assert len(tb_[0]) == 1
    assert len(tb_[1]) == 1
    assert len(tb_[2]) == 1
    assert len(tb_[3]) == 1
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert ks_tb_['myid3'] == old_ks_tb_['myid3']
    assert ks_tb_['myid4'] == old_ks_tb_['myid4']
    assert 'myid5' in ks_tb_.keys()
    assert ks_tb_['myid5'] == 'e'
    
def test_deepreplace():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']))
    tb_ = db_.table()
    ks_tb_ = db_._storage.read()['keystore']
    
    tb_.insert({'myid1':1})
    
    assert len(tb_) == 1
    assert len(ks_tb_) == 1
    assert 'myid1' in ks_tb_.keys()
    assert ks_tb_['myid1'] == 'a'
    old_ks_tb_ = ks_tb_.copy()
    
    tb_.insert({'myid1':{'myid2':2}})
    
    assert len(tb_) == 2
    assert len(ks_tb_) == 2
    assert 'myid2' in ks_tb_.keys()
    assert ks_tb_['myid2'] == 'b'
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    old_ks_tb_ = ks_tb_.copy()
    
    tb_.insert({'myid1':{'myid2':{'myid3':3}}})
    
    assert len(tb_) == 3
    assert len(ks_tb_) == 3
    assert 'myid3' in ks_tb_.keys()
    assert ks_tb_['myid3'] == 'c'
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    old_ks_tb_ = ks_tb_.copy()
    
    tb_.insert({'myid1':{'myid2':{'myid3':{'myid4':4}}}})
    
    assert len(tb_) == 4
    assert len(ks_tb_) == 4
    assert 'myid4' in ks_tb_.keys()
    assert ks_tb_['myid4'] == 'd'
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert ks_tb_['myid3'] == old_ks_tb_['myid3']
    old_ks_tb_ = ks_tb_.copy()
    
    tb_.insert({'myid1':{'myid2':{'myid3':{'myid4':{'myid5':5}}}}})
    
    assert len(tb_) == 5
    assert len(ks_tb_) == 5
    assert 'myid5' in ks_tb_.keys()
    assert ks_tb_['myid5'] == 'e'
    assert ks_tb_['myid1'] == old_ks_tb_['myid1']
    assert ks_tb_['myid2'] == old_ks_tb_['myid2']
    assert ks_tb_['myid3'] == old_ks_tb_['myid3']
    assert ks_tb_['myid4'] == old_ks_tb_['myid4']

