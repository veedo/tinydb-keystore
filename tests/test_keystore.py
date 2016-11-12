from tinydb import where
from tinydb.database import TinyDB
from tinydb_keystore import KeystoreMiddleware
from tinydb.storages import MemoryStorage

def test_newtable_addone(db_middleware_empty):
    """
    Test that adding a single element works in a new table
    """
    db_ = db_middleware_empty
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

def test_newtable_addtwo_removefirst(db_middleware_empty):
    """
    Test that adds and removes elements
    """
    db_ = db_middleware_empty
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
    
    print(tb_.all())
    print(ks_tb_)
    
    tb_.insert({'myid1':1})
    
    print(tb_.all())
    print(ks_tb_)

    assert len(tb_) == 1
    assert len(ks_tb_) == 1
    assert 'myid1' in ks_tb_.keys()
    assert ks_tb_['myid1'] == 'a'
    
    tb_.insert({'myid2':5})

    assert len(tb_) == 2
    assert len(ks_tb_) == 2
    assert 'myid2' in ks_tb_.keys()
    assert ks_tb_['myid2'] == 'b'

