from tinydb import where

import pytest

def test_newtable_addone(db_middleware_empty):
    """
    Test that adding a single element works in a new table
    """
    db = db_middleware_empty
    table = db.table('someothertable')
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
    db = db_middleware_empty
    table = db.table('someothertable')
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
