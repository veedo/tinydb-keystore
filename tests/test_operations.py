"""
From TinyDB
https://github.com/msiemens/tinydb/tree/master/tests
"""

from tinydb import where
from tinydb.operations import delete, increment, decrement
from conftest import db_middleware_populated_withkeylist, db_middleware_populated
import pytest

@pytest.mark.parametrize('db', [db_middleware_populated_withkeylist(), db_middleware_populated()])
def test_delete(db):
    db.update(delete('int'), where('char') == 'a')
    assert 'int' not in db.get(where('char') == 'a')


@pytest.mark.parametrize('db', [db_middleware_populated_withkeylist(), db_middleware_populated()])
def test_increment(db):
    db.update(increment('int'), where('char') == 'a')
    assert db.get(where('char') == 'a')['int'] == 2


@pytest.mark.parametrize('db', [db_middleware_populated_withkeylist(), db_middleware_populated()])
def test_decrement(db):
    db.update(decrement('int'), where('char') == 'a')
    assert db.get(where('char') == 'a')['int'] == 0
