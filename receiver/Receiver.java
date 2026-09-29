import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Persistent-identity interactive subscriber for the JSON-lines broker. */
public final class Receiver {
    private static final long RECONNECT_DELAY_MS = 3000;
    private static final BlockingQueue<InputLine> CONSOLE_INPUT = new LinkedBlockingQueue<>();
    private static volatile boolean consoleReaderStarted;

    private static final class InputLine {
        final String line;
        final boolean eof;
        InputLine(String line, boolean eof) { this.line = line; this.eof = eof; }
    }

    private static final class BrokerEvent {
        final String line;
        final boolean closed;
        BrokerEvent(String line, boolean closed) { this.line = line; this.closed = closed; }
    }

    private Receiver() {}

    public static void main(String[] args) throws InterruptedException {
        String host = "127.0.0.1", user = null;
        int port = 9000;
        boolean noReconnect = false;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--host" -> host = args[++i];
                    case "--port" -> port = Integer.parseInt(args[++i]);
                    case "--user" -> user = args[++i];
                    case "--no-reconnect" -> noReconnect = true;
                    case "--help", "-h" -> { usage(); return; }
                    default -> throw new IllegalArgumentException("unknown option: " + args[i]);
                }
            }
            if (user == null || user.isBlank()) throw new IllegalArgumentException("--user is required");
            run(host, port, user, noReconnect);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            usage();
            System.exit(2);
        }
    }

    private static void usage() {
        System.out.println("Usage: java Receiver [--host HOST] [--port PORT] --user USER [--no-reconnect]");
    }

    private static void run(String host, int port, String user, boolean noReconnect) throws InterruptedException {
        startConsoleReader();
        while (true) {
            boolean quit;
            try {
                quit = session(host, port, user);
            } catch (SecurityException e) {
                System.out.println("[login failed] " + e.getMessage());
                return;
            } catch (IOException e) {
                System.out.println("\n[connection lost: " + e.getMessage() + "]");
                quit = false;
            }
            if (quit || noReconnect) return;
            System.out.println("[reconnecting in 3s...]");
            Thread.sleep(RECONNECT_DELAY_MS);
        }
    }

    /** Returns true if the user asked to quit or stdin reached EOF. */
    private static boolean session(String host, int port, String user) throws IOException, InterruptedException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 10000);
        socket.setTcpNoDelay(true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        BlockingQueue<BrokerEvent> brokerEvents = new LinkedBlockingQueue<>();
        try {
            send(out, Map.of("type", "login", "user", user));
            String line = in.readLine();
            if (line == null) throw new IOException("broker closed the connection during login");
            Map<String, Object> response = Json.object(line);
            if (!"login_ok".equals(response.get("type")))
                throw new SecurityException(String.valueOf(response.getOrDefault("message", "login rejected: " + response)));
            System.out.println("Logged in as '" + user + "'.");
            Object restored = response.get("subscribed_topics");
            if (restored instanceof List<?> topics && !topics.isEmpty()) System.out.println("Restored subscriptions: " + topics);

            Thread reader = new Thread(() -> readMessages(in, brokerEvents), "broker-reader");
            reader.setDaemon(true);
            reader.start();
            System.out.println("Type '?' for help");
            prompt();
            while (true) {
                BrokerEvent event = brokerEvents.poll();
                if (event != null) {
                    if (event.closed) throw new IOException("broker closed the connection");
                    printBrokerMessage(event.line);
                }
                InputLine input = CONSOLE_INPUT.poll(100, TimeUnit.MILLISECONDS);
                if (input == null) continue;
                if (input.eof) return true;
                String trimmed = input.line.trim();
                if (trimmed.isEmpty()) { prompt(); continue; }
                String[] parts = trimmed.split("\\s+");
                String cmd = parts[0].toLowerCase(Locale.ROOT);
                Map<String, Object> msg;
                switch (cmd) {
                    case "#", "subscribe" -> {
                        if (parts.length < 2) { System.out.println("usage: # <topic> [topic ...]"); prompt(); continue; }
                        msg = new LinkedHashMap<>(); msg.put("type", "subscribe"); msg.put("topics", Arrays.asList(parts).subList(1, parts.length));
                    }
                    case "!", "unsubscribe" -> {
                        if (parts.length < 2) { System.out.println("usage: ! <topic> [topic ...]"); prompt(); continue; }
                        msg = new LinkedHashMap<>(); msg.put("type", "unsubscribe"); msg.put("topics", Arrays.asList(parts).subList(1, parts.length));
                    }
                    case "topics" -> msg = Map.of("type", "list_topics");
                    case "backlog" -> {
                        Object topic = null;
                        if (parts.length >= 3 && parts[1].equals("--topic")) topic = parts[2];
                        else if (parts.length == 2) topic = parts[1];
                        msg = new LinkedHashMap<>(); msg.put("type", "backlog"); msg.put("topic", topic);
                    }
                    case "?" -> { help(); prompt(); continue; }
                    case "quit" -> { return true; }
                    default -> { System.out.println("unknown command"); prompt(); continue; }
                }
                try { send(out, msg); prompt(); }
                catch (IOException e) { throw new IOException("connection to broker was lost: " + e.getMessage(), e); }
            }
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static void startConsoleReader() {
        if (consoleReaderStarted) return;
        consoleReaderStarted = true;
        Thread console = new Thread(() -> {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null) CONSOLE_INPUT.put(new InputLine(line, false));
            } catch (IOException | InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally { CONSOLE_INPUT.offer(new InputLine(null, true)); }
        }, "console-reader");
        console.setDaemon(true);
        console.start();
    }

    private static void send(BufferedWriter out, Map<String, ?> value) throws IOException {
        out.write(Json.stringify(value)); out.write('\n'); out.flush();
    }

    private static void readMessages(BufferedReader in, BlockingQueue<BrokerEvent> events) {
        try {
            String line;
            while ((line = in.readLine()) != null) events.put(new BrokerEvent(line, false));
        } catch (IOException | InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally { events.offer(new BrokerEvent(null, true)); }
    }

    private static void printBrokerMessage(String line) {
        Map<String, Object> msg;
        try { msg = Json.object(line); } catch (RuntimeException e) { return; }
        switch (String.valueOf(msg.get("type"))) {
            case "message" -> {
                String tag = Boolean.TRUE.equals(msg.get("backlog")) ? "backlog:" + msg.get("topic") : String.valueOf(msg.get("topic"));
                Object payload = msg.containsKey("payload") ? msg.get("payload") : msg.get("content");
                Object publisher = msg.containsKey("publisher") ? msg.get("publisher") : msg.get("sender");
                System.out.printf("\n[%s] %s  (from %s)%n", tag, integerIfNumeric(payload), publisher);
            }
            case "ack" -> System.out.printf("%n[ack] %s%n", msg);
            case "topics" -> System.out.printf("%n[topics] %s%n", msg.get("topics"));
            case "backlog_notice" -> System.out.printf("%nYou have %s messages from %s%n", integerIfNumeric(msg.get("count")), msg.get("topic"));
            case "error" -> System.out.printf("%n[error] %s%n", msg.get("message"));
        }
        prompt();
    }

    private static Object integerIfNumeric(Object value) {
        if (value instanceof String s && s.matches("-?(0|[1-9][0-9]*)")) {
            try { return Integer.valueOf(s); } catch (NumberFormatException ignored) { return value; }
        }
        if (value instanceof Long n && n >= Integer.MIN_VALUE && n <= Integer.MAX_VALUE) return n.intValue();
        return value;
    }

    private static void prompt() { System.out.print("> "); System.out.flush(); }
    private static void help() {
        System.out.println("  # <t> / subscribe <t>       subscribe to topic <t>");
        System.out.println("  ! <t> / unsubscribe <t>     unsubscribe from topic <t>");
        System.out.println("  topics                      list all available topics");
        System.out.println("  backlog [--topic <t>]       show backlog (all topics if omitted)");
        System.out.println("  ?                           show this help");
        System.out.println("  quit                        disconnect and exit");
    }

    /** Small dependency-free JSON reader/writer for the broker's JSON-lines protocol. */
    private static final class Json {
        static Map<String, Object> object(String s) {
            Object value = new Parser(s).parse();
            if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("JSON object expected");
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(String.valueOf(k), v));
            return result;
        }
        static String stringify(Object value) {
            if (value == null) return "null";
            if (value instanceof String s) return quote(s);
            if (value instanceof Number || value instanceof Boolean) return value.toString();
            if (value instanceof Map<?, ?> map) {
                StringJoiner j = new StringJoiner(",", "{", "}");
                map.forEach((k, v) -> j.add(quote(String.valueOf(k)) + ":" + stringify(v)));
                return j.toString();
            }
            if (value instanceof Iterable<?> items) {
                StringJoiner j = new StringJoiner(",", "[", "]");
                for (Object item : items) j.add(stringify(item));
                return j.toString();
            }
            throw new IllegalArgumentException("unsupported JSON value: " + value.getClass());
        }
        private static String quote(String s) {
            StringBuilder b = new StringBuilder("\"");
            for (char c : s.toCharArray()) {
                switch (c) {
                    case '"' -> b.append("\\\""); case '\\' -> b.append("\\\\");
                    case '\b' -> b.append("\\b"); case '\f' -> b.append("\\f");
                    case '\n' -> b.append("\\n"); case '\r' -> b.append("\\r"); case '\t' -> b.append("\\t");
                    default -> { if (c < 0x20) b.append(String.format("\\u%04x", (int)c)); else b.append(c); }
                }
            }
            return b.append('"').toString();
        }
        private static final class Parser {
            final String s; int i;
            Parser(String s) { this.s = s; }
            Object parse() { Object v = value(); ws(); if (i != s.length()) fail(); return v; }
            Object value() {
                ws(); if (i >= s.length()) return fail(); char c = s.charAt(i);
                if (c == '"') return string();
                if (c == '{') { i++; Map<String,Object> m = new LinkedHashMap<>(); ws(); if (take('}')) return m; do { ws(); String k = string(); ws(); require(':'); m.put(k, value()); ws(); if (take('}')) return m; require(','); } while (true); }
                if (c == '[') { i++; List<Object> a = new ArrayList<>(); ws(); if (take(']')) return a; do { a.add(value()); ws(); if (take(']')) return a; require(','); } while (true); }
                if (s.startsWith("true", i)) { i += 4; return true; }
                if (s.startsWith("false", i)) { i += 5; return false; }
                if (s.startsWith("null", i)) { i += 4; return null; }
                int start = i; if (take('-')) {} while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                if (take('.')) while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) { i++; if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++; while (i < s.length() && Character.isDigit(s.charAt(i))) i++; }
                if (start == i) return fail(); String n = s.substring(start, i);
                try { return n.contains(".") || n.contains("e") || n.contains("E") ? Double.parseDouble(n) : Long.parseLong(n); }
                catch (NumberFormatException e) { return fail(); }
            }
            String string() {
                require('"'); StringBuilder b = new StringBuilder();
                while (i < s.length()) { char c = s.charAt(i++); if (c == '"') return b.toString();
                    if (c == '\\') { if (i >= s.length()) return fail(); char e = s.charAt(i++); switch (e) {
                        case '"', '\\', '/' -> b.append(e); case 'b' -> b.append('\b'); case 'f' -> b.append('\f'); case 'n' -> b.append('\n'); case 'r' -> b.append('\r'); case 't' -> b.append('\t');
                        case 'u' -> { if (i + 4 > s.length()) return fail(); try { b.append((char)Integer.parseInt(s.substring(i, i + 4), 16)); } catch (NumberFormatException ex) { return fail(); } i += 4; }
                        default -> { return fail(); }
                    }} else { if (c < 0x20) return fail(); b.append(c); }
                } return fail();
            }
            void ws() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
            boolean take(char c) { if (i < s.length() && s.charAt(i) == c) { i++; return true; } return false; }
            void require(char c) { if (!take(c)) fail(); }
            <T> T fail() { throw new IllegalArgumentException("invalid JSON at offset " + i); }
        }
    }
}
