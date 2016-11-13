from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage

import pytest

from tinydb_keystore import KeystoreMiddleware

def populate_db_simple(db):
    db.insert_multiple({'int': 1, 'char': c} for c in 'abc')

"""
PyTest fixture for TinyDB tests
"""
@pytest.fixture
def db_middleware_empty():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage))
    db_.purge_tables()
    return db_

@pytest.fixture
def db_middleware_populated():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage))
    db_.purge_tables()
    populate_db_simple(db_)
    return db_

@pytest.fixture
def db_middleware_empty_withkeylist():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']))
    db_.purge_tables()
    return db_

@pytest.fixture
def db_middleware_populated_withkeylist():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']))
    db_.purge_tables()
    populate_db_simple(db_)
    return db_

if __name__ == '__main__':
    pytest.main()