from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage

import pytest

from tinydb_keystore import KeystoreMiddleware

def populate_db_simple(db):
    db.insert_multiple({'intvalue': 1, 'charvalue': c} for i in [1, 2, 3, 4, 5] for c in 'abcde')

"""
PyTest fixture for TinyDB tests
"""
@pytest.fixture
def db():
    db_ = TinyDB(storage=KeystoreMiddleware(MemoryStorage))
    db_.purge_tables()
    db_.insert_multiple({'int': 1, 'char': c} for c in 'abc')
    return db_
    
@pytest.fixture
def db_middleware_empty():
    with TinyDB(storage=KeystoreMiddleware(MemoryStorage)) as p_db:
        p_db.purge_tables()
        
        yield p_db

@pytest.fixture
def db_middleware_populated():
    with TinyDB(storage=KeystoreMiddleware(MemoryStorage)) as p_db:
        p_db.purge_tables()
        populate_db_simple(p_db)

        yield p_db

if __name__ == '__main__':
    pytest.main()