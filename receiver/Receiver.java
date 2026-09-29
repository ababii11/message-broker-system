import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple TCP subscriber for the message broker.
 *
 * Wire format: one UTF-8 JSON object per line. The receiver logs in, subscribes
 * to the requested topics, and then prints every line sent by the broker.
 */
public final class Receiver {
    private static final Pattern TYPE_FIELD = Pattern.compile("\\\"type\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");

    private Receiver() { }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java Receiver <username> <topic> [topic ...] [--host HOST] [--port PORT]");
            System.err.println("Example: java Receiver student1 weather news --host 127.0.0.1 --port 9000");
            System.exit(2);
        }

        String username = args[0];
        String host = "127.0.0.1";
        int port = 9000;
        List<String> topics = new ArrayList<>();

        for (int i = 1; i < args.length; i++) {
            if ("--host".equals(args[i])) {
                if (++i >= args.length) usageError("--host requires a value");
                host = args[i];
            } else if ("--port".equals(args[i])) {
                if (++i >= args.length) usageError("--port requires a value");
                try {
                    port = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    usageError("port must be a number");
                }
            } else if (args[i].startsWith("--")) {
                usageError("unknown option: " + args[i]);
            } else if (!args[i].trim().isEmpty()) {
                topics.add(args[i].trim());
            }
        }
        if (topics.isEmpty()) usageError("provide at least one topic");
        if (port < 1 || port > 65535) usageError("port must be between 1 and 65535");

        try {
            run(host, port, username, topics);
        } catch (IOException e) {
            System.err.println("Receiver connection failed: " + e.getMessage());
            System.err.println("Check that the broker is running and that the host and port are correct.");
            System.exit(1);
        }
    }

    private static void run(String host, int port, String username, List<String> topics) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setKeepAlive(true);
            try (BufferedReader input = new BufferedReader(new InputStreamReader(
                         socket.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                         socket.getOutputStream(), StandardCharsets.UTF_8))) {

                send(output, "{\"type\":\"login\",\"user\":" + jsonString(username) + "}");
                String loginReply = input.readLine();
                if (loginReply == null) throw new IOException("broker closed the connection during login");
                printBrokerLine(loginReply);
                String loginType = messageType(loginReply);
                if (!"login_ok".equals(loginType)) {
                    throw new IOException("login was rejected by broker");
                }

                StringBuilder subscription = new StringBuilder("{\"type\":\"subscribe\",\"topics\":[");
                for (int i = 0; i < topics.size(); i++) {
                    if (i > 0) subscription.append(',');
                    subscription.append(jsonString(topics.get(i)));
                }
                subscription.append("]}");
                send(output, subscription.toString());
                // Pull messages queued while this persistent subscriber was offline.
                send(output, "{\"type\":\"backlog\"}");

                System.out.println("Connected to " + host + ":" + port + "; listening on " + String.join(", ", topics));
                String line;
                while ((line = input.readLine()) != null) {
                    printBrokerLine(line);
                }
                System.out.println("Broker closed the connection.");
            }
        }
    }

    private static void send(BufferedWriter output, String json) throws IOException {
        output.write(json);
        output.newLine();
        output.flush();
    }

    private static void printBrokerLine(String line) {
        String type = messageType(line);
        if ("message".equals(type)) {
            System.out.println("[MESSAGE] " + line);
        } else if ("error".equals(type) || "login_error".equals(type)) {
            System.err.println("[BROKER ERROR] " + line);
        } else {
            System.out.println("[BROKER] " + line);
        }
    }

    private static String messageType(String json) {
        Matcher matcher = TYPE_FIELD.matcher(json);
        return matcher.find() ? matcher.group(1) : "";
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

    private static void usageError(String message) {
        System.err.println("Error: " + message);
        System.err.println("Usage: java Receiver <username> <topic> [topic ...] [--host HOST] [--port PORT]");
        System.exit(2);
    }
}
