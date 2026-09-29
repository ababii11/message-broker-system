import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Interactive Java subscriber for the repository's newline-delimited JSON broker. */
public final class Receiver {
    private static final double RECONNECT_DELAY_SECONDS = 3.0;
    private static final String END_OF_INPUT = new String("<end-of-input>");
    private static final Pattern TYPE_FIELD = Pattern.compile("\"type\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern COUNT_FIELD = Pattern.compile("\"count\"\\s*:\\s*(\\d+)");

    private Receiver() { }

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 9000;
        String user = null;
        boolean reconnect = true;

        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--host": host = requireValue(args, ++i, "--host"); break;
                    case "--port": port = Integer.parseInt(requireValue(args, ++i, "--port")); break;
                    case "--user": user = requireValue(args, ++i, "--user"); break;
                    case "--no-reconnect": reconnect = false; break;
                    case "--help": usage(); return;
                    default: throw new IllegalArgumentException("unknown option: " + args[i]);
                }
            }
            if (user == null || user.trim().isEmpty()) throw new IllegalArgumentException("--user is required");
            if (port < 1 || port > 65535) throw new IllegalArgumentException("port must be between 1 and 65535");
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            usage();
            System.exit(2);
            return;
        }

        BlockingQueue<String> commands = new LinkedBlockingQueue<>();
        Thread consoleReader = new Thread(() -> readConsole(commands), "receiver-console-input");
        consoleReader.setDaemon(true);
        consoleReader.start();

        while (true) {
            try {
                if (session(host, port, user, commands)) return;
            } catch (LoginRejectedException e) {
                System.err.println("[login failed] " + e.getMessage());
                return;
            } catch (IOException e) {
                print("\n[connection lost: " + e.getMessage() + "]");
            }

            if (!reconnect) return;
            print(String.format("[reconnecting in %.0f seconds...]", RECONNECT_DELAY_SECONDS));
            try {
                Thread.sleep((long) (RECONNECT_DELAY_SECONDS * 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Runs one login session. Returns true when the user explicitly exits. */
    private static boolean session(String host, int port, String user, BlockingQueue<String> commands)
            throws IOException, LoginRejectedException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setKeepAlive(true);
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            send(output, "{\"type\":\"login\",\"user\":" + jsonString(user) + "}");
            String loginReply = input.readLine();
            if (loginReply == null) throw new IOException("broker closed the connection during login");
            String loginType = messageType(loginReply);
            if (!"login_ok".equals(loginType)) {
                throw new LoginRejectedException(jsonStringField(loginReply, "message", loginReply));
            }

            print("Logged in as '" + user + "'.");
            String restored = stringArrayField(loginReply, "subscribed_topics");
            if (!restored.isEmpty()) print("Restored subscriptions: " + restored);

            ExecutorService readerExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "receiver-broker-reader");
                thread.setDaemon(true);
                return thread;
            });
            Future<?> readerTask = readerExecutor.submit(() -> readBroker(input));
            print("Type '?' for help");

            try {
                while (!readerTask.isDone()) {
                    String line;
                    try {
                        line = commands.poll(200, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return true;
                    }
                    if (line == null) continue;
                    if (line == END_OF_INPUT || line.equals("quit")) return true;
                    sendCommand(output, line);
                }
                try {
                    readerTask.get();
                    throw new IOException("broker closed the connection");
                } catch (java.util.concurrent.ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException) throw (IOException) cause;
                    throw new IOException("error reading from broker", cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while reading from broker", e);
                }
            } finally {
                readerExecutor.shutdownNow();
            }
        }
    }

    private static void readConsole(BlockingQueue<String> commands) {
        try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            while (true) {
                print("> ");
                String line = console.readLine();
                if (line == null) {
                    commands.offer(END_OF_INPUT);
                    return;
                }
                commands.put(line.trim());
            }
        } catch (IOException e) {
            commands.offer(END_OF_INPUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void readBroker(BufferedReader input) {
        try {
            String line;
            while ((line = input.readLine()) != null) printBrokerLine(line);
            throw new IOException("broker closed the connection");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendCommand(BufferedWriter output, String line) throws IOException {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return;
        String[] parts = trimmed.split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) return;
        String command = parts[0].toLowerCase();
        String json;

        switch (command) {
            case "#":
            case "subscribe":
                if (parts.length < 2) { print("usage: # <topic> [topic2 ...]"); return; }
                json = topicCommand("subscribe", Arrays.copyOfRange(parts, 1, parts.length));
                break;
            case "!":
            case "unsubscribe":
                if (parts.length < 2) { print("usage: ! <topic> [topic2 ...]"); return; }
                json = topicCommand("unsubscribe", Arrays.copyOfRange(parts, 1, parts.length));
                break;
            case "topics":
                json = "{\"type\":\"list_topics\"}";
                break;
            case "backlog":
                String topic = null;
                if (parts.length == 2) topic = parts[1];
                else if (parts.length == 3 && "--topic".equals(parts[1])) topic = parts[2];
                else if (parts.length > 1) { print("usage: backlog [--topic <topic>]"); return; }
                json = topic == null ? "{\"type\":\"backlog\"}"
                        : "{\"type\":\"backlog\",\"topic\":" + jsonString(topic) + "}";
                break;
            case "?":
            case "help":
                help();
                return;
            case "quit":
                return;
            default:
                print("unknown command; type '?' for help");
                return;
        }
        send(output, json);
    }

    private static String topicCommand(String type, String[] topics) {
        StringBuilder json = new StringBuilder("{\"type\":").append(jsonString(type)).append(",\"topics\":[");
        for (int i = 0; i < topics.length; i++) {
            if (i > 0) json.append(',');
            json.append(jsonString(topics[i]));
        }
        return json.append("]}").toString();
    }

    private static void printBrokerLine(String line) {
        String type = messageType(line);
        switch (type) {
            case "message":
                String topic = jsonStringField(line, "topic", "?");
                boolean backlog = line.contains("\"backlog\": true") || line.contains("\"backlog\":true");
                String payload = displayJsonValue(jsonValueField(line, "payload",
                        jsonValueField(line, "content", "null")));
                String publisher = jsonStringField(line, "publisher", jsonStringField(line, "sender", "unknown"));
                print("\n[" + (backlog ? "backlog:" : "") + topic + "] " + payload
                        + "  (from " + publisher + ")\n> ");
                break;
            case "ack": print("\n[ack] " + line + "\n> "); break;
            case "topics": print("\n[topics] " + line + "\n> "); break;
            case "backlog_notice":
                Matcher count = COUNT_FIELD.matcher(line);
                print("\nYou have " + (count.find() ? count.group(1) : "some") + " messages from "
                        + jsonStringField(line, "topic", "?") + "\n> ");
                break;
            case "error":
                print("\n[error] " + jsonStringField(line, "message", line) + "\n> ");
                break;
            default: print("\n[broker] " + line + "\n> ");
        }
    }

    private static String messageType(String json) {
        Matcher matcher = TYPE_FIELD.matcher(json);
        return matcher.find() ? matcher.group(1) : "";
    }

    private static String jsonStringField(String json, String field, String fallback) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
        Matcher matcher = pattern.matcher(json);
        return matcher.find() ? unescape(matcher.group(1)) : fallback;
    }

    /** Returns a JSON value as its original text, preserving objects and arrays. */
    private static String jsonValueField(String json, String field, String fallback) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) return fallback;
        int start = matcher.end();
        if (start >= json.length()) return fallback;
        char first = json.charAt(start);
        if (first == '\"') {
            boolean escaped = false;
            for (int i = start + 1; i < json.length(); i++) {
                char c = json.charAt(i);
                if (c == '\"' && !escaped) return json.substring(start, i + 1);
                if (c == '\\' && !escaped) escaped = true;
                else escaped = false;
            }
            return fallback;
        }
        if (first == '{' || first == '[') {
            int depth = 0;
            boolean inString = false;
            boolean escaped = false;
            for (int i = start; i < json.length(); i++) {
                char c = json.charAt(i);
                if (inString) {
                    if (c == '\"' && !escaped) inString = false;
                    if (c == '\\' && !escaped) escaped = true;
                    else escaped = false;
                } else if (c == '\"') {
                    inString = true;
                } else if (c == '{' || c == '[') {
                    depth++;
                } else if (c == '}' || c == ']') {
                    depth--;
                    if (depth == 0) return json.substring(start, i + 1);
                }
            }
            return fallback;
        }
        int end = start;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
        return json.substring(start, end).trim();
    }

    private static String displayJsonValue(String value) {
        if (value.length() >= 2 && value.charAt(0) == '\"' && value.charAt(value.length() - 1) == '\"') {
            return unescape(value.substring(1, value.length() - 1));
        }
        return value;
    }

    private static String stringArrayField(String json, String field) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\\[([^]]*)\\]");
        Matcher matcher = pattern.matcher(json);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    private static String unescape(String value) {
        return value.replace("\\\"", "\"").replace("\\\\", "\\")
                .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
    }

    private static String jsonString(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"': result.append("\\\""); break;
                case '\\': result.append("\\\\"); break;
                case '\b': result.append("\\b"); break;
                case '\f': result.append("\\f"); break;
                case '\n': result.append("\\n"); break;
                case '\r': result.append("\\r"); break;
                case '\t': result.append("\\t"); break;
                default:
                    if (c < 0x20) result.append(String.format("\\u%04x", (int) c));
                    else result.append(c);
            }
        }
        return result.append('"').toString();
    }

    private static void send(BufferedWriter output, String json) throws IOException {
        output.write(json);
        output.newLine();
        output.flush();
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) throw new IllegalArgumentException(option + " requires a value");
        return args[index];
    }

    private static void help() {
        print("  # <topic> [topic2 ...]   subscribe to topics");
        print("  ! <topic> [topic2 ...]   unsubscribe from topics");
        print("  topics                   list available topics");
        print("  backlog [--topic <t>]    fetch queued messages");
        print("  ?                        show this help");
        print("  quit                     disconnect and exit");
    }

    private static void usage() {
        System.out.println("Usage: java Receiver --user USER [--host HOST] [--port PORT] [--no-reconnect]");
        System.out.println("Defaults: --host 127.0.0.1 --port 9000; reconnect every 3 seconds.");
    }

    private static synchronized void print(String text) {
        System.out.print(text);
        System.out.flush();
    }

    private static final class LoginRejectedException extends Exception {
        LoginRejectedException(String message) { super(message); }
    }
}
