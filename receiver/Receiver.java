import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Persistent-identity interactive subscriber for the JSON-lines broker. */
public final class Receiver {
    private static final long RECONNECT_DELAY_MS = 3000;

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
    private static boolean session(String host, int port, String user) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 10000);
        socket.setTcpNoDelay(true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        try {
            send(out, Map.of("type", "login", "user", user));
            String line = in.readLine();
            if (line == null) throw new IOException("broker closed the connection during login");
            Map<String, Object> response = Json.object(line);
            if (!"login_ok".equals(response.get("type"))) {
                throw new SecurityException(String.valueOf(response.getOrDefault("message", "login rejected: " + response)));
            }
            System.out.println("Logged in as '" + user + "'.");
            Object restored = response.get("subscribed_topics");
            if (restored instanceof List<?> topics && !topics.isEmpty())
                System.out.println("Restored subscriptions: " + topics);

            Thread reader = new Thread(() -> readMessages(in), "broker-reader");
            reader.setDaemon(true);
            reader.start();
            System.out.println("Type '?' for help");
            try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String input;
                while ((input = console.readLine()) != null) {
                    String trimmed = input.trim();
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
                    send(out, msg);
                    prompt();
                }
                return true;
            }
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static void send(BufferedWriter out, Map<String, ?> value) throws IOException {
        out.write(Json.stringify(value)); out.write('\n'); out.flush();
    }

    private static void readMessages(BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                Map<String, Object> msg;
                try { msg = Json.object(line); } catch (RuntimeException e) { continue; }
                String type = String.valueOf(msg.get("type"));
                switch (type) {
                    case "message" -> {
                        String tag = Boolean.TRUE.equals(msg.get("backlog")) ? "backlog:" + msg.get("topic") : String.valueOf(msg.get("topic"));
                        System.out.printf("\n[%s] %s  (from %s)%n> ", tag, msg.get("payload"), msg.get("publisher"));
                    }
                    case "ack" -> System.out.printf("%n[ack] %s%n> ", msg);
                    case "topics" -> System.out.printf("%n[topics] %s%n> ", msg.get("topics"));
                    case "backlog_notice" -> System.out.printf("%nYou have %s messages from %s%n> ", msg.get("count"), msg.get("topic"));
                    case "error" -> System.out.printf("%n[error] %s%n> ", msg.get("message"));
                }
                System.out.flush();
            }
            System.out.println("\n[broker connection closed]");
        } catch (IOException e) {
            System.out.println("\n[broker connection lost: " + e.getMessage() + "]");
        }
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
