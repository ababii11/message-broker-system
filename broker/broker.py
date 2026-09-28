import argparse
import asyncio
import json
import logging
import socket
import sqlite3
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Set

LOG = logging.getLogger("broker")

HISTORY_LIMIT = 20  # last N messages kept in RAM per topic, replayed on a *fresh* subscribe


@dataclass
class ClientConn:
    """Tracks one connected socket: its (optional) logged-in username and
    the topics it's currently receiving live pushes for."""
    writer: asyncio.StreamWriter
    peer: str
    username: Optional[str] = None
    topics: Set[str] = field(default_factory=set)


class Broker:
    def __init__(self, db_path: str):
        self.clients: Dict[asyncio.StreamWriter, ClientConn] = {}
        self.subscribers: Dict[str, Set[asyncio.StreamWriter]] = defaultdict(set)
        self.online: Dict[str, ClientConn] = {}  # username -> connection, only while connected
        self.history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=HISTORY_LIMIT))

        self.lock = asyncio.Lock()      # guards the in-memory dicts above
        self.db_lock = asyncio.Lock()   # serializes access to self.conn

        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

    # ---------------------------------------------------------------- #
    # SQLite helpers
    # ---------------------------------------------------------------- #

    def _init_db(self):
        self.conn.executescript(
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
        self.conn.commit()
        self.conn.execute("UPDATE users SET connected = 0")
        self.conn.commit()

    async def _db(self, fn, *args):
        async with self.db_lock:
            return await asyncio.to_thread(fn, *args)

    def _exec(self, query: str, params=()):
        cur = self.conn.execute(query, params)
        self.conn.commit()
        return cur.lastrowid

    def _executemany(self, query: str, seq):
        self.conn.executemany(query, seq)
        self.conn.commit()

    def _query(self, query: str, params=()):
        return self.conn.execute(query, params).fetchall()

    # ---------------------------------------------------------------- #
    # Connection lifecycle
    # ---------------------------------------------------------------- #

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        peer_str = f"{peer[0]}:{peer[1]}" if peer else "unknown"

        sock = writer.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except OSError:
                pass

        conn = ClientConn(writer=writer, peer=peer_str)
        self.clients[writer] = conn
        LOG.info("Client connected: %s", peer_str)

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break  # EOF: peer closed the socket
                await self._process_line(conn, line)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        except Exception:
            LOG.exception("Unexpected error handling client %s", peer_str)
        finally:
            await self._disconnect(conn)

    async def _disconnect(self, conn: ClientConn):
        async with self.lock:
            self.clients.pop(conn.writer, None)
            for topic in conn.topics:
                self.subscribers[topic].discard(conn.writer)
            if conn.username:
                self.online.pop(conn.username, None)

        if conn.username:
            await self._db(
                self._exec,
                "UPDATE users SET connected = 0, last_seen = ? WHERE username = ?",
                (datetime.now(timezone.utc).isoformat(), conn.username),
            )

        try:
            conn.writer.close()
            await conn.writer.wait_closed()
        except Exception:
            pass
        LOG.info("Client disconnected: %s (user=%s)", conn.peer, conn.username)

    # ---------------------------------------------------------------- #
    # Message dispatch
    # ---------------------------------------------------------------- #

    async def _process_line(self, conn: ClientConn, raw: bytes):
        try:
            text = raw.decode("utf-8").strip()
            if not text:
                return
            msg = json.loads(text)
            if not isinstance(msg, dict):
                raise ValueError("top-level JSON must be an object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
            await self._send(conn.writer, {"type": "error", "message": f"invalid message: {e}"})
            return

        handler = {
            "login": self._on_login,
            "register_topic": self._on_register_topic,
            "subscribe": self._on_subscribe,
            "unsubscribe": self._on_unsubscribe,
            "list_topics": self._on_list_topics,
            "publish": self._on_publish,
            "message": self._on_publish,  # accepted alias
            "backlog": self._on_backlog,
        }.get(msg.get("type"))

        if handler is None:
            await self._send(conn.writer, {"type": "error", "message": f"unknown type '{msg.get('type')}'"})
            return

        try:
            await handler(conn, msg)
        except Exception as e:
            LOG.exception("Error handling message from %s", conn.peer)
            await self._send(conn.writer, {"type": "error", "message": str(e)})

    # ---------------------------------------------------------------- #
    # login — persistent subscriber identity
    # ---------------------------------------------------------------- #

    async def _on_login(self, conn: ClientConn, msg: dict):
        username = msg.get("user")
        if not username or not isinstance(username, str):
            await self._send(conn.writer, {"type": "error", "message": "login requires a 'user' string"})
            return
        if conn.username:
            await self._send(conn.writer, {"type": "error", "message": f"already logged in as '{conn.username}'"})
            return

        async with self.lock:
            if username in self.online:
                await self._send(conn.writer, {
                    "type": "login_error",
                    "message": f"user '{username}' is already connected elsewhere",
                })
                return
            self.online[username] = conn  # reserve the name

        try:
            conn.username = username
            await self._db(
                self._exec,
                "INSERT INTO users(username, connected, last_seen) VALUES (?, 1, ?) "
                "ON CONFLICT(username) DO UPDATE SET connected = 1, last_seen = excluded.last_seen",
                (username, datetime.now(timezone.utc).isoformat()),
            )

            rows = await self._db(self._query, "SELECT topic FROM subscriptions WHERE username = ?", (username,))
            topics = sorted(r[0] for r in rows)
            async with self.lock:
                for t in topics:
                    self.subscribers[t].add(conn.writer)
                    conn.topics.add(t)

            await self._send(conn.writer, {"type": "login_ok", "user": username, "subscribed_topics": topics})

            backlog_counts = await self._db(
                self._query,
                "SELECT topic, COUNT(*) FROM backlog WHERE username = ? AND delivered = 0 GROUP BY topic",
                (username,),
            )
            for topic, count in backlog_counts:
                await self._send(conn.writer, {"type": "backlog_notice", "topic": topic, "count": count})

        except Exception:
            async with self.lock:
                self.online.pop(username, None)
            conn.username = None
            raise

    # ---------------------------------------------------------------- #
    # register_topic — a publisher announces a topic exists, so
    # subscribers can discover and subscribe to it even before the first
    # actual message is published
    # ---------------------------------------------------------------- #

    async def _on_register_topic(self, conn: ClientConn, msg: dict):
        topic = msg.get("topic")
        if not topic:
            await self._send(conn.writer, {"type": "error", "message": "register_topic requires 'topic'"})
            return
        await self._db(
            self._exec,
            "INSERT OR IGNORE INTO topics(topic, registered_at) VALUES (?, ?)",
            (topic, datetime.now(timezone.utc).isoformat()),
        )
        await self._send(conn.writer, {"type": "ack", "action": "register_topic", "topic": topic, "status": "ok"})

    # ---------------------------------------------------------------- #
    # subscribe / unsubscribe — persisted per username
    # ---------------------------------------------------------------- #

    async def _on_subscribe(self, conn: ClientConn, msg: dict):
        if not conn.username:
            await self._send(conn.writer, {"type": "error", "message": "log in first: {'type':'login','user':'...'}"})
            return
        topics = self._extract_topics(msg)
        if not topics:
            await self._send(conn.writer, {"type": "error", "message": "subscribe requires 'topics' (list) or 'topic' (str)"})
            return

        registered_rows = await self._db(self._query, "SELECT topic FROM topics")
        registered = {r[0] for r in registered_rows}
        valid = [t for t in topics if t in registered]
        invalid = [t for t in topics if t not in registered]

        if valid:
            await self._db(
                self._executemany,
                "INSERT OR IGNORE INTO subscriptions(username, topic) VALUES (?, ?)",
                [(conn.username, t) for t in valid],
            )
            async with self.lock:
                for t in valid:
                    self.subscribers[t].add(conn.writer)
                    conn.topics.add(t)

            await self._send(conn.writer, {"type": "ack", "action": "subscribe", "topics": valid, "status": "ok"})
            # Replay recent live history for topics that are new to this user.
            for t in valid:
                for past in list(self.history[t]):
                    await self._send(conn.writer, past)

        if invalid:
            await self._send(conn.writer, {
                "type": "error",
                "message": f"unknown topic(s): {', '.join(invalid)} — no publisher has registered or "
                           f"published to them yet. Use 'topics' to see what's available.",
            })

    async def _on_unsubscribe(self, conn: ClientConn, msg: dict):
        if not conn.username:
            await self._send(conn.writer, {"type": "error", "message": "log in first: {'type':'login','user':'...'}"})
            return
        topics = self._extract_topics(msg)
        await self._db(
            self._executemany,
            "DELETE FROM subscriptions WHERE username = ? AND topic = ?",
            [(conn.username, t) for t in topics],
        )
        async with self.lock:
            for t in topics:
                self.subscribers[t].discard(conn.writer)
                conn.topics.discard(t)
        await self._send(conn.writer, {"type": "ack", "action": "unsubscribe", "topics": topics, "status": "ok"})

    async def _on_list_topics(self, conn: ClientConn, msg: dict):
        rows = await self._db(self._query, "SELECT topic FROM topics ORDER BY topic")
        topics = [r[0] for r in rows]
        await self._send(conn.writer, {"type": "topics", "topics": topics})

    # ---------------------------------------------------------------- #
    # publish — log durably, push to online subscribers, backlog the rest
    # ---------------------------------------------------------------- #

    async def _on_publish(self, conn: ClientConn, msg: dict):
        topic = msg.get("topic")
        if not topic:
            await self._send(conn.writer, {"type": "error", "message": "publish requires 'topic'"})
            return
        content = msg.get("content", msg.get("payload", ""))
        sender_name = msg.get("sender", conn.peer)
        
        # Extragem timestamp din mesajul primit, sau generăm unul nou
        timestamp = msg.get("timestamp", datetime.now(timezone.utc).isoformat())
        
        envelope = {
            "type": "message",
            "topic": topic,
            "content": content,
            "sender": sender_name,
            "timestamp": timestamp,
        }

        message_id = await self._db(
            self._exec,
            "INSERT INTO messages(topic, payload, publisher, timestamp) VALUES (?, ?, ?, ?)",
            (topic, json.dumps(content), sender_name, timestamp),
        )
        await self._db(
            self._exec,
            "INSERT OR IGNORE INTO topics(topic, registered_at) VALUES (?, ?)",
            (topic, timestamp),
        )

        async with self.lock:
            self.history[topic].append(envelope)
            live_targets = list(self.subscribers.get(topic, ()))
            online_usernames = set(self.online.keys())

        sent = 0
        for w in live_targets:
            if await self._send(w, envelope):
                sent += 1

        # Anyone persistently subscribed but not currently online gets a backlog row.
        subscriber_rows = await self._db(self._query, "SELECT username FROM subscriptions WHERE topic = ?", (topic,))
        offline_users = [u for (u,) in subscriber_rows if u not in online_usernames]
        if offline_users:
            await self._db(
                self._executemany,
                "INSERT INTO backlog(username, message_id, topic, delivered) VALUES (?, ?, ?, 0)",
                [(u, message_id, topic) for u in offline_users],
            )

        await self._send(conn.writer, {
            "type": "ack", "action": "publish", "topic": topic,
            "status": "ok", "subscribers_notified": sent, "backlogged_for": len(offline_users),
        })

    # ---------------------------------------------------------------- #
    # backlog — pull queued messages from while a user was offline
    # ---------------------------------------------------------------- #

    async def _on_backlog(self, conn: ClientConn, msg: dict):
        if not conn.username:
            await self._send(conn.writer, {"type": "error", "message": "log in first: {'type':'login','user':'...'}"})
            return
        topic = msg.get("topic")

        if topic:
            rows = await self._db(
                self._query,
                "SELECT b.id, m.topic, m.payload, m.publisher, m.timestamp "
                "FROM backlog b JOIN messages m ON b.message_id = m.id "
                "WHERE b.username = ? AND b.delivered = 0 AND b.topic = ? ORDER BY m.id",
                (conn.username, topic),
            )
        else:
            rows = await self._db(
                self._query,
                "SELECT b.id, m.topic, m.payload, m.publisher, m.timestamp "
                "FROM backlog b JOIN messages m ON b.message_id = m.id "
                "WHERE b.username = ? AND b.delivered = 0 ORDER BY m.id",
                (conn.username,),
            )

        delivered_ids = []
        for backlog_id, mtopic, payload_json, publisher, timestamp in rows:
            envelope = {
                "type": "message",
                "topic": mtopic,
                "payload": json.loads(payload_json),
                "publisher": publisher,
                "timestamp": timestamp,
                "backlog": True,
            }
            await self._send(conn.writer, envelope)
            delivered_ids.append(backlog_id)

        if delivered_ids:
            await self._db(
                self._executemany,
                "UPDATE backlog SET delivered = 1 WHERE id = ?",
                [(i,) for i in delivered_ids],
            )

        await self._send(conn.writer, {
            "type": "ack", "action": "backlog", "topic": topic, "count": len(delivered_ids),
        })

    # ---------------------------------------------------------------- #
    # shared helpers
    # ---------------------------------------------------------------- #

    @staticmethod
    def _extract_topics(msg: dict) -> List[str]:
        if isinstance(msg.get("topics"), list):
            return [str(t) for t in msg["topics"]]
        if msg.get("topic"):
            return [str(msg["topic"])]
        return []

    async def _send(self, writer: asyncio.StreamWriter, obj: dict) -> bool:
        try:
            writer.write((json.dumps(obj) + "\n").encode("utf-8"))
            await writer.drain()
            return True
        except (ConnectionResetError, BrokenPipeError):
            return False


async def main():
    parser = argparse.ArgumentParser(description="Pub/Sub message broker with persistent subscribers")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--db", default="broker_database.db", help="SQLite file (':memory:' for a non-persistent run)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    broker = Broker(db_path=args.db)
    server = await asyncio.start_server(broker.handle_client, args.host, args.port)

    addr = ", ".join(str(s.getsockname()) for s in server.sockets)
    LOG.info("Broker listening on %s (db=%s)", addr, args.db)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
