import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import messagebroker.rpc.*;

/** Persistent-identity interactive subscriber for the gRPC broker. */
public final class Receiver {
    private static final long RECONNECT_DELAY_MS = 3000;
    private static final BlockingQueue<InputLine> CONSOLE_INPUT = new LinkedBlockingQueue<>();
    private static volatile boolean consoleReaderStarted;

    private static final class InputLine {
        final String line;
        final boolean eof;

        InputLine(String line, boolean eof) {
            this.line = line;
            this.eof = eof;
        }
    }

    private static final class BrokerEvent {
        final Event event;
        final boolean closed;

        BrokerEvent(Event event, boolean closed) {
            this.event = event;
            this.closed = closed;
        }
    }

    private Receiver() {
    }

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
                    case "--help", "-h" -> {
                        usage();
                        return;
                    }
                    default -> throw new IllegalArgumentException("unknown option: " + args[i]);
                }
            }
            if (user == null || user.isBlank())
                throw new IllegalArgumentException("--user is required");
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
            if (quit || noReconnect)
                return;
            System.out.println("[reconnecting in 3s...]");
            Thread.sleep(RECONNECT_DELAY_MS);
        }
    }

    /** Returns true if the user asked to quit or stdin reached EOF. */
    private static boolean session(String host, int port, String user) throws IOException, InterruptedException {
        // shortcut: plaintext channel, no auth; use TLS + credentials before leaving a
        // trusted LAN
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        BrokerGrpc.BrokerBlockingStub stub = BrokerGrpc.newBlockingStub(channel);
        BlockingQueue<BrokerEvent> brokerEvents = new LinkedBlockingQueue<>();
        try {
            // Connect is login + live feed in one: the stream stays open while we are
            // online.
            Iterator<Event> stream = stub.connect(ConnectRequest.newBuilder().setUser(user).build());
            Event welcome;
            try {
                welcome = stream.next();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.UNAVAILABLE)
                    throw new IOException(e.getMessage(), e);
                throw new SecurityException(e.getStatus().getDescription());
            }
            System.out.println("Logged in as '" + user + "'.");
            Set<String> subscribedTopics = new LinkedHashSet<>(welcome.getWelcome().getSubscribedTopicsList());
            Map<String, Integer> backlogCounts = new LinkedHashMap<>();
            if (!subscribedTopics.isEmpty())
                System.out.println("Restored subscriptions: " + subscribedTopics);

            Thread reader = new Thread(() -> readEvents(stream, brokerEvents), "broker-reader");
            reader.setDaemon(true);
            reader.start();
            System.out.println("Type '?' for help");
            prompt();
            while (true) {
                BrokerEvent event = brokerEvents.poll();
                if (event != null) {
                    if (event.closed)
                        throw new IOException("broker closed the connection");
                    printEvent(event.event, backlogCounts);
                }
                InputLine input = CONSOLE_INPUT.poll(100, TimeUnit.MILLISECONDS);
                if (input == null)
                    continue;
                if (input.eof)
                    return true;
                String trimmed = input.line.trim();
                if (trimmed.isEmpty()) {
                    prompt();
                    continue;
                }
                String[] parts = trimmed.split("\\s+");
                String cmd = parts[0].toLowerCase(Locale.ROOT);
                List<String> args = Arrays.asList(parts).subList(1, parts.length);
                BrokerGrpc.BrokerBlockingStub rpc = stub.withDeadlineAfter(10, TimeUnit.SECONDS);
                try {
                    switch (cmd) {
                        case "*" -> System.out.println(
                                "Subscribed topics: " + (subscribedTopics.isEmpty() ? "none" : subscribedTopics));
                        case "#", "subscribe" -> {
                            if (args.isEmpty())
                                System.out.println("usage: # <topic> [topic ...]");
                            else {
                                SubscribeReply reply = rpc
                                        .subscribe(UserTopics.newBuilder().setUser(user).addAllTopics(args).build());
                                subscribedTopics.addAll(reply.getSubscribedList());
                                System.out.printf("%n[ack] subscribed %s%n", reply.getSubscribedList());
                                if (reply.getUnknownCount() > 0)
                                    System.out.printf("%n[error] unknown topic(s): %s%n", reply.getUnknownList());
                            }
                        }
                        case "!", "unsubscribe" -> {
                            if (args.isEmpty())
                                System.out.println("usage: ! <topic> [topic ...]");
                            else {
                                rpc.unsubscribe(UserTopics.newBuilder().setUser(user).addAllTopics(args).build());
                                subscribedTopics.removeAll(args);
                                System.out.printf("%n[ack] unsubscribed %s%n", args);
                            }
                        }
                        case "topics" -> System.out.printf("%n[topics] %s%n",
                                rpc.listTopics(Empty.getDefaultInstance()).getTopicsList());
                        case "backlog" -> {
                            if (args.isEmpty()) {
                                if (backlogCounts.isEmpty())
                                    System.out.println("You have no backlogged messages.");
                                else
                                    backlogCounts.forEach((topic, count) -> System.out
                                            .printf("You have %d messages from %s%n", count, topic));
                            } else if (args.equals(List.of("--all"))
                                    || (args.size() == 2 && args.get(0).equals("--topic"))) {
                                String topic = args.size() == 2 ? args.get(1) : "";
                                int delivered = 0;
                                // plain stub: a large backlog may take longer than the 10s command deadline
                                Iterator<Message> pending = stub
                                        .backlog(BacklogRequest.newBuilder().setUser(user).setTopic(topic).build());
                                while (pending.hasNext()) {
                                    printMessage(pending.next(), backlogCounts);
                                    delivered++;
                                }
                                System.out.printf("%n[ack] backlog delivered %d%n", delivered);
                            } else
                                System.out.println("usage: backlog | backlog --all | backlog --topic <topic>");
                        }
                        case "?" -> help();
                        case "quit" -> {
                            return true;
                        }
                        default -> System.out.println("unknown command");
                    }
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.UNAVAILABLE)
                        throw new IOException("connection to broker was lost: " + e.getMessage(), e);
                    System.out.printf("%n[error] %s%n", e.getMessage());
                }
                prompt();
            }
        } finally {
            channel.shutdownNow();
        }
    }

    private static void startConsoleReader() {
        if (consoleReaderStarted)
            return;
        consoleReaderStarted = true;
        Thread console = new Thread(() -> {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null)
                    CONSOLE_INPUT.put(new InputLine(line, false));
            } catch (IOException | InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                CONSOLE_INPUT.offer(new InputLine(null, true));
            }
        }, "console-reader");
        console.setDaemon(true);
        console.start();
    }

    private static void readEvents(Iterator<Event> stream, BlockingQueue<BrokerEvent> events) {
        try {
            while (stream.hasNext())
                events.put(new BrokerEvent(stream.next(), false));
        } catch (StatusRuntimeException | InterruptedException ignored) {
            // the closed marker below tells the main loop to reconnect
        } finally {
            events.offer(new BrokerEvent(null, true));
        }
    }

    private static void printEvent(Event event, Map<String, Integer> backlogCounts) {
        switch (event.getKindCase()) {
            case NOTICE -> {
                BacklogNotice notice = event.getNotice();
                if (notice.getCount() > 0)
                    backlogCounts.put(notice.getTopic(), notice.getCount());
                System.out.printf("%nYou have %d messages from %s%n", notice.getCount(), notice.getTopic());
            }
            case MESSAGE -> printMessage(event.getMessage(), backlogCounts);
            default -> {
            }
        }
        prompt();
    }

    private static void printMessage(Message msg, Map<String, Integer> backlogCounts) {
        if (msg.getBacklog())
            backlogCounts.computeIfPresent(msg.getTopic(), (topic, left) -> left <= 1 ? null : left - 1);
        String tag = msg.getBacklog() ? "backlog:" + msg.getTopic() : msg.getTopic();
        System.out.printf("%n[%s] %s  (from %s)%n", tag, msg.getContent(), msg.getSender());
    }

    private static void prompt() {
        System.out.print("> ");
        System.out.flush();
    }

    private static void help() {
        System.out.println("  *                           show your subscribed topics");
        System.out.println("  # <t> / subscribe <t>       subscribe to topic <t>");
        System.out.println("  ! <t> / unsubscribe <t>     unsubscribe from topic <t>");
        System.out.println("  topics                      list all available topics");
        System.out.println("  backlog                     show pending counts by topic");
        System.out.println("  backlog --all               show all backlogged messages");
        System.out.println("  backlog --topic <t>         show backlogged messages for topic <t>");
        System.out.println("  ?                           show this help");
        System.out.println("  quit                        disconnect and exit");
    }
}
