import argparse
import asyncio
import logging
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, Optional

import grpc
from google.protobuf import empty_pb2

# Compiles broker.proto at import time (needs grpcio-tools); broker.proto sits next to this file.
pb, pb_grpc = grpc.protos_and_services("broker.proto")

LOG = logging.getLogger("broker")

HISTORY_LIMIT = 20  # last N messages kept in RAM per topic, replayed on a *fresh* subscribe
# concurrent read-only SQLite connections (WAL lets these run alongside the writer)
READ_POOL_SIZE = 4
MAX_NAME = 128      # longest accepted user / topic name


def valid_name(name: str) -> bool:
    return 0 < len(name.strip()) <= MAX_NAME


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def require(context, ok: bool, detail: str):
    # Trust boundary: protobuf checks field types, this checks the values.
    if not ok:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, detail)


class Broker(pb_grpc.BrokerServicer):
    def __init__(self, db_path: str):
        # username -> queue feeding that user's open Connect stream.
        # shortcut: queue is unbounded, a stalled client grows RAM; bound it and spill to backlog when that matters.
        self.online: Dict[str, asyncio.Queue] = {}
        self.history: Dict[str, Deque] = defaultdict(
            lambda: deque(maxlen=HISTORY_LIMIT))

        # serializes WRITES only - SQLite allows just one writer
        self.write_lock = asyncio.Lock()

        self.db_path = db_path
        self._write_conn = sqlite3.connect(db_path, check_same_thread=False)

        self._read_pool: Optional[asyncio.Queue] = None
        if db_path != ":memory:":
            self._write_conn.execute("PRAGMA journal_mode=WAL")
            self._write_conn.execute("PRAGMA busy_timeout=5000")
            self._read_pool = asyncio.Queue()
            for _ in range(READ_POOL_SIZE):
                read_conn = sqlite3.connect(
                    f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
                read_conn.execute("PRAGMA busy_timeout=5000")
                self._read_pool.put_nowait(read_conn)

        self._init_db()

    # ---------------------------------------------------------------- #
    # SQLite helpers
    # ---------------------------------------------------------------- #

    def _init_db(self):
        self._write_conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                username   TEXT PRIMARY KEY,
                connected  INTEGER NOT NULL DEFAULT 0,
                last_seen  TEXT
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                username TEXT NOT NULL,
                topic    TEXT NOT NULL,
                PRIMARY KEY (username, topic)
            );
            CREATE TABLE IF NOT EXISTS messages (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                topic     TEXT NOT NULL,
                payload   TEXT NOT NULL,
                publisher TEXT,
                timestamp TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS backlog (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username   TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                topic      TEXT NOT NULL,
                delivered  INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS topics (
                topic         TEXT PRIMARY KEY,
                registered_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_backlog_user_pending
                ON backlog(username, delivered);
            """
        )
        self._write_conn.commit()
        self._write_conn.execute("UPDATE users SET connected = 0")
        self._write_conn.commit()

    async def _db_write(self, fn, *args):
        # sqlite allows one writer at a time, serialize all writes through a single connection
        async with self.write_lock:
            return await asyncio.to_thread(fn, self._write_conn, *args)

    async def _db_read(self, fn, *args):
        # Check out a connection from the read pool (if any) and run a read-only query on it
        # This allows multiple concurrent reads to happen in parallel, even while a write is happening
        if self._read_pool is None:  # ':memory:' fallback: no separate pool
            async with self.write_lock:
                return await asyncio.to_thread(fn, self._write_conn, *args)
        conn = await self._read_pool.get()
        try:
            return await asyncio.to_thread(fn, conn, *args)
        finally:
            self._read_pool.put_nowait(conn)

    @staticmethod
    def _exec(conn: sqlite3.Connection, query: str, params=()):
        cur = conn.execute(query, params)
        conn.commit()
        return cur.lastrowid

    @staticmethod
    def _executemany(conn: sqlite3.Connection, query: str, seq):
        conn.executemany(query, seq)
        conn.commit()

    @staticmethod
    def _query(conn: sqlite3.Connection, query: str, params=()):
        return conn.execute(query, params).fetchall()

    # ---------------------------------------------------------------- #
    # RPCs. grpc.aio runs every call as its own coroutine, so calls are concurrent;
    # `online`/`history` are only touched between awaits, so they need no lock.
    # ---------------------------------------------------------------- #

    async def _require_online(self, context, user: str):
        # shortcut: `user` is an unauthenticated claim (before, identity was bound to the socket), add auth/tokens before untrusted networks.
        if user not in self.online:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "call Connect(user) first and keep that stream open")

    async def Hello(self, request, context):
        await require(context, valid_name(request.user), f"user must be 1-{MAX_NAME} non-blank characters")
        LOG.info("Sender connected: %s (%s)", request.user, context.peer())
        return empty_pb2.Empty()

    async def Publish(self, request, context):
        await require(context, valid_name(request.topic), f"topic must be 1-{MAX_NAME} non-blank characters")
        topic, sender, timestamp = request.topic, request.sender or context.peer(), now_iso()
        message = pb.Message(topic=topic, content=request.content,
                             sender=sender, timestamp=timestamp)

        message_id = await self._db_write(
            self._exec,
            "INSERT INTO messages(topic, payload, publisher, timestamp) VALUES (?, ?, ?, ?)",
            (topic, request.content, sender, timestamp),
        )
        await self._db_write(self._exec, "INSERT OR IGNORE INTO topics(topic, registered_at) VALUES (?, ?)", (topic, timestamp))
        self.history[topic].append(message)

        rows = await self._db_read(self._query, "SELECT username FROM subscriptions WHERE topic = ?", (topic,))
        subscribers = [row[0] for row in rows]
        live = [u for u in subscribers if u in self.online]
        offline = [u for u in subscribers if u not in self.online]
        for user in live:
            self.online[user].put_nowait(pb.Event(message=message))
        if offline:
            await self._db_write(
                self._executemany,
                "INSERT INTO backlog(username, message_id, topic, delivered) VALUES (?, ?, ?, 0)",
                [(user, message_id, topic) for user in offline],
            )
        return pb.PublishReply(subscribers_notified=len(live), backlogged_for=len(offline))

    async def Connect(self, request, context):
        user = request.user
        await require(context, valid_name(user), f"user must be 1-{MAX_NAME} non-blank characters")
        if user in self.online:  # no await between this check and the assignment below, so it is atomic
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, f"user '{user}' is already connected elsewhere")
        queue = self.online[user] = asyncio.Queue()
        LOG.info("Receiver connected: %s (%s)", user, context.peer())
        try:
            await self._db_write(
                self._exec,
                "INSERT INTO users(username, connected, last_seen) VALUES (?, 1, ?) "
                "ON CONFLICT(username) DO UPDATE SET connected = 1, last_seen = excluded.last_seen",
                (user, now_iso()),
            )
            rows = await self._db_read(self._query, "SELECT topic FROM subscriptions WHERE username = ?", (user,))
            yield pb.Event(welcome=pb.Welcome(subscribed_topics=sorted(row[0] for row in rows)))

            counts = await self._db_read(
                self._query,
                "SELECT topic, COUNT(*) FROM backlog WHERE username = ? AND delivered = 0 GROUP BY topic",
                (user,),
            )
            for topic, count in counts:
                yield pb.Event(notice=pb.BacklogNotice(topic=topic, count=count))

            while True:  # ends when the client goes away: grpc cancels this coroutine at the await
                yield await queue.get()
        finally:
            LOG.info("Receiver disconnected: %s", user)
            self.online.pop(user, None)
            await self._db_write(
                self._exec, "UPDATE users SET connected = 0, last_seen = ? WHERE username = ?", (now_iso(
                ), user)
            )

    async def Subscribe(self, request, context):
        await self._require_online(context, request.user)
        rows = await self._db_read(self._query, "SELECT topic FROM topics")
        registered = {row[0] for row in rows}
        valid = [t for t in request.topics if t in registered]
        unknown = [t for t in request.topics if t not in registered]
        if valid:
            await self._db_write(
                self._executemany,
                "INSERT OR IGNORE INTO subscriptions(username, topic) VALUES (?, ?)",
                [(request.user, t) for t in valid],
            )
            # may have disconnected during the awaits above
            queue = self.online.get(request.user)
            if queue:
                for topic in valid:
                    for past in self.history[topic]:
                        queue.put_nowait(pb.Event(message=past))
        return pb.SubscribeReply(subscribed=valid, unknown=unknown)

    async def Unsubscribe(self, request, context):
        await self._require_online(context, request.user)
        await self._db_write(
            self._executemany,
            "DELETE FROM subscriptions WHERE username = ? AND topic = ?",
            [(request.user, t) for t in request.topics],
        )
        return empty_pb2.Empty()

    async def ListTopics(self, request, context):
        rows = await self._db_read(self._query, "SELECT topic FROM topics ORDER BY topic")
        return pb.Topics(topics=[row[0] for row in rows])

    async def Backlog(self, request, context):
        await self._require_online(context, request.user)
        query = (
            "SELECT b.id, m.topic, m.payload, m.publisher, m.timestamp "
            "FROM backlog b JOIN messages m ON b.message_id = m.id "
            "WHERE b.username = ? AND b.delivered = 0"
        )
        params = (request.user,)
        if request.topic:
            query, params = query + \
                " AND b.topic = ?", params + (request.topic,)
        rows = await self._db_read(self._query, query + " ORDER BY m.id", params)

        sent = []
        try:
            for backlog_id, topic, content, sender, timestamp in rows:
                yield pb.Message(topic=topic, content=content, sender=sender, timestamp=timestamp, backlog=True)
                sent.append(backlog_id)
        finally:  # a client that drops mid-stream only consumes what it actually received
            if sent:
                await self._db_write(
                    self._executemany, "UPDATE backlog SET delivered = 1 WHERE id = ?", [
                        (i,) for i in sent]
                )


async def main():
    parser = argparse.ArgumentParser(
        description="Pub/Sub message broker (gRPC) with persistent subscribers")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--db", default="broker_database.db",
                        help="SQLite file (':memory:' for a non-persistent run)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    # HTTP/2 pings drop dead peers, so a crashed client cannot hold its username forever.
    server = grpc.aio.server(
        options=[("grpc.keepalive_time_ms", 30000), ("grpc.keepalive_timeout_ms", 10000)])
    pb_grpc.add_BrokerServicer_to_server(Broker(args.db), server)
    # shortcut: plaintext HTTP/2, add TLS (add_secure_port) before leaving a trusted LAN.
    server.add_insecure_port(f"{args.host}:{args.port}")
    await server.start()
    LOG.info("Broker listening on %s:%s (db=%s)",
             args.host, args.port, args.db)
    await server.wait_for_termination()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
