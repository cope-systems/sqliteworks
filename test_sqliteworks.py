import pytest
import tempfile
import os

import sqliteworks


@pytest.fixture()
def db_directory():
    with tempfile.TemporaryDirectory(prefix='sqliteworks-test-') as d:
        yield d


@pytest.fixture
def db_path(db_directory):
    yield os.path.join(db_directory, 'test.db')


@pytest.fixture
def db_conn(db_path):
    yield sqliteworks.create_connection(db_path)



@pytest.fixture
def basic_queue(db_conn):
    queue = sqliteworks.SQLiteWorkQueue('basic_queue', db_conn)
    queue.init()
    yield queue


@pytest.fixture
def basic_kv_store(db_conn):
    kv_store = sqliteworks.SQLiteKVStore('basic_kv', db_conn)
    kv_store.init()
    yield kv_store


def test_basic_queue_behavior(db_conn, basic_queue):
    item = basic_queue.push('test-data')
    assert isinstance(item, sqliteworks.WorkQueueItem)
    assert item.state == sqliteworks.SQLiteWorkQueueStates.QUEUED
    assert isinstance(item.item_id, int)

    item_again = basic_queue.pop_queued()
    assert item_again is not None
    assert item_again.data == 'test-data'
    assert item_again.item_id == item.item_id
    assert item_again.state == sqliteworks.SQLiteWorkQueueStates.IN_PROGRESS

    item = basic_queue.get_item_by_id(item.item_id)
    assert item is not None
    assert item_again.data == 'test-data'
    assert item_again.item_id == item.item_id
    assert item.state == sqliteworks.SQLiteWorkQueueStates.IN_PROGRESS

    basic_queue.mark_item(item, sqliteworks.SQLiteWorkQueueStates.COMPLETED)

    item = basic_queue.get_item_by_id(item.item_id)
    assert item is not None
    assert item_again.data == 'test-data'
    assert item_again.item_id == item.item_id
    assert item.state == sqliteworks.SQLiteWorkQueueStates.COMPLETED

    assert basic_queue.purge_old_items(seconds_old=-1) == 1
    item = basic_queue.get_item_by_id(item.item_id)
    assert item is None


def test_basic_kv_store_behavior(db_conn, basic_kv_store):
    assert len(basic_kv_store) == 0
    assert basic_kv_store.count() == 0

    basic_kv_store['foo'] = [1, 2, 3]
    assert basic_kv_store['foo'] == [1, 2, 3]

    assert set(basic_kv_store.keys()) == {'foo'}

    del basic_kv_store['foo']

    assert set(basic_kv_store.keys()) == set()
