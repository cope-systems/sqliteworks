import json
import threading
from typing import Optional, Any, Iterator, Tuple, Mapping, Union, Dict, List, ContextManager
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
import random

from sqlite3 import dbapi2 as sqlite


DEFAULT_CONNECTION_PRAGMAS = [
    "PRAGMA cache_size=4096",
    "PRAGMA synchronous=1"
]

DATABASE_CREATION_PRAGMAS = [
    "PRAGMA page_size=4096",
    "PRAGMA auto_vacuum=1",
    "PRAGMA mmap_size=16777216",
    "PRAGMA journal_mode=wal"
]


class TransactionProxy(object):
    """
    A SQlite3 connection wrapper, to indicate that the connection is in a transaction.
    This is used to facilitate a "deferred rollback" and attach other attributes to the
    transactions that can not be adds as attributes to the underlying connection.
    """
    def __init__(self, _connection):
        self._connection = _connection
        self._deferred_rollback = False

    def __getattr__(self, item):
        return getattr(self._connection, item)

    def __setitem__(self, key, value):
        return setattr(self._connection, key, value)

    @classmethod
    def deferred_rollback(cls, transaction: 'TransactionProxy') -> 'TransactionProxy':
        setattr(transaction, '_trigger_rollback', True)
        return transaction


deferred_rollback = TransactionProxy.deferred_rollback
Connectionish = Union[sqlite.Connection, TransactionProxy]

class TransactionTypes(object):
    DEFERRED = 'DEFERRED'
    IMMEDIATE = 'IMMEDIATE'
    EXCLUSIVE = 'EXCLUSIVE'


@contextmanager
def transaction_wrapper(connection: Connectionish, commit_on_exc_types: Optional[Union[List, Tuple]] = None,
                        transaction_type: str = TransactionTypes.DEFERRED) \
        -> ContextManager[TransactionProxy]:
    """
    Open a transaction on the current connection. At the end of the context, if there are no
    exceptions, the transaction is committed. If there is an exception, or the deferred rollback mechanism
    was set, the transaction will be rolled back.

    :param connection:
    :param commit_on_exc_types: If not None, if an exception of any of the types specified is caught, the transaction
      is committed instead of rolled back. This is useful for web frameworks that use exceptions for things like
      HTTP redirects, where the developer still needs to commit.
    :param transaction_type: What type of SQLite3 transaction is used. Default is deferred, other choices are
      immediate and exclusive.
    :return: The transaction context.
    """
    txn = TransactionProxy(connection)
    try:
        txn.execute("BEGIN {0} TRANSACTION".format(transaction_type))
        yield txn
    except tuple(commit_on_exc_types or []):
        txn.commit()
        raise
    except BaseException:
        txn.rollback()
        raise
    else:
        if getattr(txn, '_trigger_rollback', False):
            txn.rollback()
            setattr(txn, '_trigger_rollback', False)
        else:
            txn.commit()


@contextmanager
def immediate_transaction_wrapper(connection: Connectionish, commit_on_exc_types: Optional[Union[List, Tuple]] = None) \
        -> ContextManager[TransactionProxy]:
    return transaction_wrapper(
        connection,
        commit_on_exc_types=commit_on_exc_types,
        transaction_type=TransactionTypes.IMMEDIATE
    )


@contextmanager
def exclusive_transaction_wrapper(connection: Connectionish, commit_on_exc_types: Optional[Union[List, Tuple]] = None) \
        -> ContextManager[TransactionProxy]:
    return transaction_wrapper(
        connection,
        commit_on_exc_types=commit_on_exc_types,
        transaction_type=TransactionTypes.EXCLUSIVE
    )


@contextmanager
def cursor_manager(connection: Connectionish) -> ContextManager[sqlite.Cursor]:
    """
    Wrap the creation and closure of a SQLite3 connection cursor as a context manager.

    :param connection: The SQLite3 connection to use.

    :return: The cursor as a context manager.
    """
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def optimize_db(connection: Connectionish):
    """
    Run PRAGMA optimize;
    """
    with cursor_manager(connection) as cursor:
        cursor.execute("PRAGMA optimize;")


def vacuum_db(connection: Connectionish):
    """
    Run a SQLite3 Vacuum
    """
    with cursor_manager(connection) as cursor:
        cursor.execute("VACUUM")


def apply_database_creation_pragmas(connection: Connectionish):
    """
    Apply a reasonable set of PRAGMAs that are
    appropriate at database creation time.
    """
    for pragma in DATABASE_CREATION_PRAGMAS:
        connection.execute(pragma)


def none_to_minus_1(v: Any) -> Any:
    """
    Convert None to minus 1, all other values are left unmolested. This is useful for queries that have
    parameterized offset or limit, as SQLite only ignores them if they are -1 instead of None (unlike
    the behavior of PostgreSQL/psycopg2, for example, where limit null or offset null mean those clauses do nothing).
    """
    if v is None:
        return -1
    else:
        return v


def opt_strip_uri_prefix(uri: str) -> str:
    """
    Convert a sqlite://database/path style URI to an actual database path consumable by
    the SQLite3 connect function. If the value isnt' a URI, pass it unmolested.
    """
    if uri.startswith("sqlite://"):
        return uri.split("sqlite://", 1)[1]
    else:
        return uri


@wraps(sqlite.connect)
def create_connection(*args, **kwargs) -> sqlite.Connection:
    """
    Create a new SQLite3 connection, with some reasonable defaults.

    This will apply some reasonable default connection pragmas (see DEFAULT_CONNECTION_PRAGMAS)
    and configure the connection to use the Dict-like sqlite.Row factory.

    All args and kwargs exception "pragmas" are passed down to the sqlite.connect function.

    :return: A SQLite3 connection.
    """
    if args:
        args = (opt_strip_uri_prefix(args[0]), *(args[1:]))
    kwargs["detect_types"] = kwargs.get("detect_types", True)
    conn = sqlite.connect(*args, **kwargs)
    conn.row_factory = sqlite.Row
    for pragma in kwargs.pop('pragmas', DEFAULT_CONNECTION_PRAGMAS):
        conn.execute(pragma)
    return conn


class SQLiteContainerWrapper(object):
    def __init__(self, connection, serialize=json.dumps, deserialize=json.loads, auto_transaction: bool =True):
        self.connection = connection
        self.serialize = serialize
        self.deserialize = deserialize
        self.auto_transaction = auto_transaction

    @contextmanager
    def maybe_auto_transaction(self, txn_type: str = TransactionTypes.IMMEDIATE) -> Connectionish:
        if self.auto_transaction:
            with transaction_wrapper(self.connection, transaction_type=txn_type) as txn:
                yield txn
        else:
            yield self.connection


class SQLiteWorkQueueStates(object):
    QUEUED = 'queued'
    IN_PROGRESS = 'in-progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    FAULTED = 'faulted'


@dataclass
class WorkQueueItem(object):
    item_id: Optional[int]
    data: Any
    state: str
    priority: int = 1
    item_ext_id: Optional[str] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None

    @classmethod
    def new(cls, data: Any, priority: int = 1,
            ext_id: Optional[str] = None, state: str = SQLiteWorkQueueStates.QUEUED) -> 'WorkQueueItem':
        return cls(
            item_id=None,
            data=data,
            state=state,
            priority=priority,
            item_ext_id=ext_id,
            created_time=int(time.time()*1000),
            last_updated_time=int(time.time()*1000)
        )

    def bump_updated_time(self):
        self.last_updated_time = int(time.time()*1000)

    def serialize(self, serializer=json.dumps) -> Dict[str, Any]:
        return {
            'item_id': self.item_id,
            'item_ext_id': self.item_ext_id,
            'item_data': serializer(self.data),
            'state_name': self.state,
            'priority': self.priority,
            'created_time': self.created_time,
            'last_updated_time': self.last_updated_time
        }

    @classmethod
    def deserialize(cls, row: Union[sqlite.Row, Mapping], deserializer=json.loads) -> Optional['WorkQueueItem']:
        if row is not None:
            return cls(
                item_id=row['item_id'],
                item_ext_id=row['item_ext_id'],
                data=deserializer(row['item_data']),
                state=row['state_name'],
                priority=row['priority'],
                created_time=row['created_time'],
                last_updated_time=row['last_updated_time']
            )
        else:
            return None


class SQLiteWorkQueue(SQLiteContainerWrapper):
    def __init__(self, queue_name, connection, serialize=json.dumps, deserialize=json.loads,
                 auto_transaction=True):
        self.queue_name = queue_name
        super().__init__(connection, serialize=serialize, deserialize=deserialize, auto_transaction=auto_transaction)

    @property
    def table_name(self):
        return "work_queue_{0}".format(self.queue_name)

    def init(self):
        self.write_schema()

    def write_schema(self):
        schema = """
        CREATE TABLE IF NOT EXISTS _queue_state(
            state_id INTEGER NOT NULL PRIMARY KEY,
            state_name TEXT UNIQUE NOT NULL,
            state_description TEXT NOT NULL DEFAULT ''
        );
        
        INSERT OR IGNORE INTO _queue_state(state_name, state_description) VALUES (
            'queued', 'The item has been queued but not yet dequeued for processing.'
        );
        INSERT OR IGNORE INTO _queue_state(state_name, state_description) VALUES (
            'in-progress', 'The item has been dequeued by a worker.'
        );
        INSERT OR IGNORE INTO _queue_state(state_name, state_description) VALUES (
            'completed', 'The item has been dequeued and successfully completed.'
        );
        INSERT OR IGNORE INTO _queue_state(state_name, state_description) VALUES (
            'failed', 'The item has been dequeued and failed while being processed.'
        );
        INSERT OR IGNORE INTO _queue_state(state_name, state_description) VALUES (
            'faulted', 'An internal error has occurred.'
        );
        
        CREATE TABLE IF NOT EXISTS {0}(
            item_id INTEGER PRIMARY KEY NOT NULL,
            item_ext_id TEXT,
            item_data TEXT NOT NULL,
            item_state_id INTEGER NOT NULL REFERENCES _queue_state(state_id),
            priority INTEGER NOT NULL DEFAULT 1,
            created_time INTEGER NOT NULL,
            last_updated_time INTEGER NOT NULL
        );
        
        CREATE UNIQUE INDEX IF NOT EXISTS {0}_ext_id_IDX ON {0}(item_ext_id) WHERE item_ext_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS {0}_state_created_IDX ON {0}(item_state_id, created_time);
        CREATE INDEX IF NOT EXISTS {0}_state_priority_created_IDX ON {0}(item_state_id, priority, created_time);
        """.format(self.table_name)
        with cursor_manager(self.connection) as c:
            c.executescript(schema)

    def push(self, data, priority=1) -> WorkQueueItem:
        new_queue_item = WorkQueueItem.new(
            data, priority=priority
        )
        with self.maybe_auto_transaction(txn_type=TransactionTypes.IMMEDIATE) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    INSERT INTO {0}(item_data, item_ext_id, item_state_id, priority, created_time, last_updated_time)
                    SELECT :item_data, :item_ext_id, state_id, :priority, :created_time, :last_updated_time
                    FROM _queue_state
                    WHERE _queue_state.state_name = :state_name
                    """.format(self.table_name),
                    new_queue_item.serialize(serializer=self.serialize)
                )
                new_queue_item.item_id = c.lastrowid
        return new_queue_item

    def pop_queued(self) -> Optional[WorkQueueItem]:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.IMMEDIATE) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    SELECT q.item_id, q.item_ext_id, q.item_data, s.state_name, q.priority, q.created_time, q.last_updated_time
                    FROM {0} AS q
                    JOIN _queue_state AS s ON q.item_state_id = s.state_id
                    WHERE s.state_name = 'queued'
                    ORDER BY q.priority DESC, q.created_time
                    LIMIT 1
                    """.format(self.table_name),
                )
                item = WorkQueueItem.deserialize(c.fetchone(), deserializer=self.deserialize)
                if item:
                    item.bump_updated_time()
                    item.state = SQLiteWorkQueueStates.IN_PROGRESS
                    c.execute(
                        """
                        UPDATE {0}
                        SET item_state_id = (SELECT state_id FROM _queue_state WHERE state_name = :state_name),
                            last_updated_time = :last_updated
                        WHERE item_id = :item_id
                        """.format(
                            self.table_name
                        ),
                        {
                            'item_id': item.item_id,
                            'state_name': item.state,
                            'last_updated': item.last_updated_time
                        }
                    )
                return item

    def peek_queued(self) -> Optional[WorkQueueItem]:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.DEFERRED) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    SELECT q.item_id, q.item_ext_id, q.item_data, s.state_name, q.priority, q.created_time, q.last_updated_time
                    FROM {0} AS q
                    JOIN _queue_state AS s ON q.item_state_id = s.state_id
                    WHERE s.state_name = 'queued'
                    ORDER BY q.priority DESC, q.created_time
                    LIMIT 1
                    """.format(self.table_name),
                )
                return WorkQueueItem.deserialize(c.fetchone(), deserializer=self.deserialize)

    def get_item_by_id(self, item_id: int) -> Optional[WorkQueueItem]:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.DEFERRED) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    SELECT q.item_id, q.item_ext_id, q.item_data, s.state_name, q.priority, q.created_time, q.last_updated_time
                    FROM {0} AS q
                    JOIN _queue_state AS s ON q.item_state_id = s.state_id
                    WHERE item_id = :item_id
                    LIMIT 1
                    """.format(self.table_name),
                    {
                        'item_id': item_id
                    }
                )
                return WorkQueueItem.deserialize(c.fetchone(), deserializer=self.deserialize)

    def get_item_by_ext_id(self, ext_id: str) -> Optional[WorkQueueItem]:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.DEFERRED) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    SELECT q.item_id, q.item_ext_id, q.item_ext_id, q.item_data, s.state_name, q.priority, q.created_time, q.last_updated_time
                    FROM {0} AS q
                    JOIN _queue_state AS s ON q.item_state_id = s.state_id
                    WHERE item_ext_id = :item_ext_id
                    LIMIT 1
                    """.format(self.table_name),
                    {
                        'item_ext_id': ext_id
                    }
                )
                return WorkQueueItem.deserialize(c.fetchone(), deserializer=self.deserialize)

    def mark_item(self, item: WorkQueueItem, new_state: str) -> WorkQueueItem:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.IMMEDIATE) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                item.bump_updated_time()
                item.state = new_state
                c.execute(
                    """
                    UPDATE {0}
                    SET item_state_id = (SELECT state_id FROM _queue_state WHERE state_name = :state_name),
                        last_updated_time = :last_updated
                    WHERE item_id = :item_id
                    """.format(
                        self.table_name
                    ),
                    {
                        'item_id': item.item_id,
                        'state_name': item.state,
                        'last_updated': item.last_updated_time
                    }
                )
                return item

    def mark_queued(self, item: WorkQueueItem) -> WorkQueueItem:
        return self.mark_item(item, SQLiteWorkQueueStates.QUEUED)

    def mark_in_progress(self, item: WorkQueueItem) -> WorkQueueItem:
        return self.mark_item(item, SQLiteWorkQueueStates.IN_PROGRESS)

    def mark_completed(self, item: WorkQueueItem) -> WorkQueueItem:
        return self.mark_item(item, SQLiteWorkQueueStates.COMPLETED)

    def mark_failed(self, item: WorkQueueItem) -> WorkQueueItem:
        return self.mark_item(item, SQLiteWorkQueueStates.FAILED)

    def purge_old_items(self, state=SQLiteWorkQueueStates.COMPLETED, seconds_old=60*60*24*7,
                        limit: Optional[int] = None) -> int:
        with self.maybe_auto_transaction(txn_type=TransactionTypes.IMMEDIATE) as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    """
                    DELETE FROM {0}
                    WHERE item_id IN (
                        SELECT item_id FROM {0}
                        WHERE item_state_id = (SELECT state_id FROM _queue_state WHERE state_name = :state_name)
                          AND last_updated_time < :ms_threshold
                        LIMIT :limit
                    );
                    """.format(
                        self.table_name
                    ),
                    {
                        'state_name': state,
                        'ms_threshold': int((time.time() - seconds_old)*1000),
                        'limit': none_to_minus_1(limit)
                    }
                )
                return c.rowcount


class SQLiteKVStore(SQLiteContainerWrapper):
    def __init__(self, kv_store_name, connection, serialize=json.dumps, deserialize=json.loads,
                 auto_transaction=True):
        self.kv_store_name = kv_store_name
        super().__init__(connection, serialize=serialize, deserialize=deserialize, auto_transaction=auto_transaction)

    @property
    def table_name(self) -> str:
        return "kv_{0}".format(self.kv_store_name)

    @staticmethod
    def key_type_check(key):
        if not isinstance(key, str):
            raise ValueError('SQLiteKVStore is only compatible with string keys.')

    def init(self):
        self.write_schema()

    def write_schema(self):
        schema = """
        CREATE TABLE IF NOT EXISTS {0}(
            key_name TEXT NOT NULL PRIMARY KEY,
            value TEXT
        );
        """.format(self.table_name)
        with cursor_manager(self.connection) as c:
            c.executescript(schema)

    def items(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Iterator[Tuple[str, Any]]:
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT key_name, value FROM {0} ORDER BY key_name LIMIT ? OFFSET ?".format(self.table_name),
                    (none_to_minus_1(limit), none_to_minus_1(offset))
                )
                for row in c:
                    yield (
                        row['key_name'],
                        self.deserialize(row['value'])
                    )

    def keys(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Iterator[str]:
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT key_name FROM {0} ORDER BY key_name LIMIT ? OFFSET ?".format(self.table_name),
                    (none_to_minus_1(limit), none_to_minus_1(offset))
                )
                for row in c:
                    yield row['key_name']

    def values(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Iterator[Any]:
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT value FROM {0} ORDER BY key_name LIMIT ? OFFSET ?".format(self.table_name),
                    (none_to_minus_1(limit), none_to_minus_1(offset))
                )
                for row in c:
                    yield self.deserialize(row['value'])

    def __iter__(self):
        return self.keys()

    def count(self) -> int:
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT COUNT(key_name) AS num FROM {0}".format(self.table_name),
                )
                return c.fetchone()['num']

    def __len__(self):
        return self.count()

    def has(self, key: str) -> bool:
        if not isinstance(key, str):
            return False
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT value FROM {0} WHERE key_name = ?".format(self.table_name),
                    (key,)
                )
                row = c.fetchone()
                return row is not None

    def __contains__(self, item):
        return self.has(item)

    def get(self, key: str, default: Any = None) -> Any:
        self.key_type_check(key)
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT value FROM {0} WHERE key_name = ?".format(self.table_name),
                    (key,)
                )
                row = c.fetchone()
                if row:
                    return self.deserialize(row['value'])
                else:
                    return default

    def __getitem__(self, item: str) -> Any:
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "SELECT value FROM {0} WHERE key_name = ?".format(self.table_name),
                    (item,)
                )
                row = c.fetchone()
                if row:
                    return self.deserialize(row['value'])
                else:
                    raise KeyError('No such key: {0}'.format(item))

    def set(self, key: str, value: Any):
        self.key_type_check(key)
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    "INSERT OR REPLACE INTO {0}(key_name, value) VALUES(?, ?)".format(self.table_name),
                    (key, self.serialize(value))
                )

    def __setitem__(self, key: str, value: Any):
        return self.set(key, value)

    def delete(self, key: str):
        self.key_type_check(key)
        with self.maybe_auto_transaction() as maybe_txn:
            with cursor_manager(maybe_txn) as c:
                c.execute(
                    'DELETE FROM {0} WHERE key_name = ?'.format(self.table_name),
                    (key,)
                )

    def __delitem__(self, key: str):
        return self.delete(key)


class ConnectionPool(object):
    """
    A very simple connection pool for a SQLite database in a multithreaded environment. The pool ensures each
    thread gets a unique connection, and that occasionally connections are occassionally closed after being returned
    (as never closing connections can occasionally lead to problems). It should a relatively similar API to the
    sqlalchemy connection pools.
    """
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._pool = {}
        self._rng = random.Random()
        self._rng.seed()

    def connect(self):
        current_thread_ident = threading.current_thread().ident
        if current_thread_ident in self._pool:
            return self._pool[current_thread_ident]
        else:
            new_conn = create_connection(*self._args, **self._kwargs)
            self._pool[current_thread_ident] = new_conn
            return new_conn

    @contextmanager
    def connection_context(self):
        conn = self.connect()
        yield conn
        self.return_connection(conn)

    def return_connection(self, conn, close_probability=0.05):
        if self._rng.uniform(0, 1) < close_probability:
            self.cleanup_for_current_thread()

    def cleanup_for_current_thread(self, raise_for_missing=False):
        current_thread_ident = threading.current_thread().ident
        if current_thread_ident in self._pool:
            return self._pool.pop(current_thread_ident).close()
        else:
            if raise_for_missing:
                raise ValueError("Thread {0} has not current connection!".format(current_thread_ident))

    def close(self):
        return self.cleanup_for_current_thread()

    def close_all(self):
        """
        This is typically useful after a fork to close all of the garbage FDs.
        """
        for thread_id in list(self._pool.keys()):
            self._pool.pop(thread_id).close()


@contextmanager
def pool_connection(pool: ConnectionPool):
    """
    Grab an appropriate connection off of a connection pool.
    """
    conn = pool.connect()
    yield conn
    pool.return_connection(conn)
