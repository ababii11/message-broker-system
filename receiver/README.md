## How it works

A single `asyncio` TCP broker that sits between senders (publishers) and
receivers (subscribers) and routes messages by **topic**.
One TCP connection per client. The number of topics is variable and decided by whoever publishes.
Many-to-many through the broker (each publisher can reach 0..N subscribers).
Invalid JSON or unknown message types return an `error` reply instead of crashing the connection or the broker (`_process_line` in `broker.py`).
Subscribers are **persistent identities** (a username, stored in
SQLite): a user's subscriptions survive disconnects, only one live
session per username is allowed at a time, and anything published while
a user is offline is queued as their personal backlog.
A topic is "registered" the moment a publisher either publishes to it or
explicitly announces it with `register_topic`.

If connection to the broker is interrupted, the users and publishers connected to it send out a 'disconnect' message and retry the connection.

The broker defaults to wildcard address 0.0.0.0, because it accepts connections from any network.
IP | Connection Type
--|--
192.168.x.x| Wi-Fi / Small office routers
10.x.x.x | Large LAN
172.17.x.x| Docker default bridge
127.0.0.1 | localhost

## Running it

Always run broker first, acts like server, knows addresses of both publishers and subscribers. (defaults to `0.0.0.0:9000`, use `--host 127.0.0.1 --port 9000`)
Use SQLite Viewer for VSCode
```bash
python broker.py --host HOST --port PORT --db DATABASE.db
```

## Wire protocol

Every message, in both directions, is **one JSON object per line**,
UTF-8 encoded, terminated with `\n`. This is why the broker reads with
`reader.readline()` - it's a simple framing scheme that both Python
(`asyncio.StreamReader.readline`) and C# (`StreamReader.ReadLineAsync`)
support natively, so no custom length-prefixing is needed.


## Example of JSON line

```json
{"type": "message", "topic": "weather", "payload": "s123", "publisher": "127.0.0.1:62863", "timestamp": "2026-09-27T08:31:33.058677+00:00"}
```

### Client → Broker

| type | fields | purpose |
|---|---|---|
| `login` | `user: str` | establish a persistent subscriber identity (required before `subscribe`/`unsubscribe`/`backlog`) |
| `subscribe` | `topics: [str]` (or `topic: str`) | start receiving messages for these topics; persisted for this user |
| `unsubscribe` | `topics: [str]` (or `topic: str`) | stop receiving messages for these topics; removed from persisted subscriptions |
| `list_topics` | — | ask which topics currently exist |
| `publish` | `topic: str`, `payload: any` | publish a message to a topic (no login needed — publishers are anonymous) |
| `backlog` | `topic: str` (optional) | pull queued messages received while offline; omit `topic` for all topics |

### Broker → Client

| type | fields | purpose |
|---|---|---|
| `login_ok` | `user`, `subscribed_topics` | login succeeded; `subscribed_topics` is restored from previous sessions |
| `login_error` | `message` | username is already connected elsewhere, or the request was malformed |
| `ack` | `action`, `topics`/`topic`, `status`, ... | confirms a subscribe/unsubscribe/publish/backlog |
| `topics` | `topics: [str]` | response to `list_topics` |
| `backlog_notice` | `topic`, `count` | sent right after `login_ok`, once per topic with pending backlog — "You have `count` messages from `topic`" |
| `message` | `topic`, `payload`, `publisher`, `timestamp`, `backlog?` | a delivered message; `backlog: true` marks one pulled from the backlog rather than delivered live |
| `error` | `message: str` | something was wrong with the last line sent (bad JSON, unknown type, not logged in, missing field) — the connection is **not** dropped |

## Java receiver

The receiver is a dependency-free Java TCP client. It logs in with a persistent
subscriber username, subscribes to the requested topics, and prints broker
events as they arrive, including messages queued while that subscriber was
offline. Compile and run it from the repository root:

```bash
javac receiver/Receiver.java
java -cp receiver Receiver student1 weather news --host 127.0.0.1 --port 9000
```

Start the broker first. A topic must have been registered or published before
the receiver subscribes; otherwise the broker returns an `error` response for
that topic. The username must not already have an active receiver connection.
