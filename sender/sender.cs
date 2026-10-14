#nullable disable
using System;
using System.Diagnostics;
using System.Threading;
using Grpc.Net.Client;
using MessageBroker.Rpc;

namespace MessageBroker.Sender
{
    public class BrokerClient : IDisposable
    {
        private GrpcChannel _channel;
        private Broker.BrokerClient _stub;
        private string _senderName;

        public void Connect(string host, int port, string senderName)
        {
            _senderName = senderName;
            // shortcut: plaintext HTTP/2, use https + TLS before leaving a trusted LAN
            _channel = GrpcChannel.ForAddress($"http://{host}:{port}");
            _stub = new Broker.BrokerClient(_channel);
            // handshake: throws if the broker is down (like TcpClient.Connect did) and lets the broker log this sender
            _stub.Hello(new ConnectRequest { User = senderName }, deadline: DateTime.UtcNow.AddSeconds(10));
        }

        public PublishReply Publish(string topic, string content) =>
            _stub.Publish(
                new Message { Sender = _senderName, Topic = topic.Trim(), Content = content.Trim() },
                deadline: DateTime.UtcNow.AddSeconds(10)); // a hung broker must not freeze this terminal forever

        public void Dispose() => _channel?.Dispose();
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Daca argumentul "child" exista, suntem intr-una din ferestrele spawnate
            if (args.Length > 0 && args[0] == "child")
            {
                int senderId = int.Parse(args[1]);
                RunInteractiveChild(senderId);
            }
            else
            {
                // Daca nu avem argumente, suntem in programul "Master" care creeaza terminalele
                RunSpawner();
            }
        }

        static int globalSenderCounter = 0;

        static void RunSpawner()
        {
            Console.WriteLine("=== MASTER SENDER ===");

            // Identificam cum a fost pornit programul (.exe direct sau prin dotnet run)
            string exePath = Process.GetCurrentProcess().MainModule.FileName;
            string dllPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            bool isDotNet = exePath.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase);

            while (true)
            {
                Console.WriteLine("\n------------------------------------------------");
                Console.Write("Introdu numarul de noi terminale cu senderi pe care doriti sa le deschideti (sau 'q' pt a iesi): ");
                string input = Console.ReadLine()?.Trim();

                if (input.ToLower() == "q" || input.ToLower() == "quit" || input.ToLower() == "exit")
                {
                    break;
                }

                if (!int.TryParse(input, out int x) || x <= 0)
                {
                    Console.WriteLine("Numar invalid. Incearca din nou.");
                    continue;
                }

                for (int i = 1; i <= x; i++)
                {
                    globalSenderCounter++;
                    ProcessStartInfo psi = new ProcessStartInfo();

                    if (isDotNet)
                    {
                        psi.FileName = exePath;
                        psi.Arguments = $"\"{dllPath}\" child {globalSenderCounter}";
                    }
                    else
                    {
                        psi.FileName = exePath;
                        psi.Arguments = $"child {globalSenderCounter}";
                    }

                    // Setarea magica pe Windows: deschide fizic o fereastra CMD noua pentru fiecare proces
                    psi.UseShellExecute = true;

                    try
                    {
                        Process.Start(psi);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Eroare la deschiderea terminalului {globalSenderCounter}: {e.Message}");
                    }
                }

                Console.WriteLine($"\n[INFO] Au fost deschise {x} ferestre noi separate (Sender {globalSenderCounter - x + 1} -> {globalSenderCounter}).");
                Console.WriteLine("Poti continua sa adaugi terminale oricand doresti direct de aici.");
            }
        }

        static void RunInteractiveChild(int senderId)
        {
            string host = "127.0.0.1";
            int brokerPort = 9000;
            string senderName = $"Sender-{senderId}";

            Console.Title = $"Terminal - {senderName}";
            Console.WriteLine(new string('=', 50));
            Console.WriteLine($" TERMINAL SENDER INTERACTIV | {senderName} ");
            Console.WriteLine(new string('=', 50));

            string topic = "";
            Mutex topicMutex = null;

            // 1. Alegerea unica a topicului per terminal
            while (true)
            {
                Console.Write("Alege Topic-ul UNIC pentru acest terminal (ex: sport): ");
                topic = Console.ReadLine()?.Trim().ToLower();

                if (string.IsNullOrEmpty(topic)) continue;
                if (topic == "exit" || topic == "quit") return;

                bool createdNew;
                // Creăm un Mutex la nivel de sistem de operare (Windows) pentru acest topic
                topicMutex = new Mutex(true, "Global\\PAD_BrokerTopic_" + topic, out createdNew);

                if (!createdNew)
                {
                    Console.WriteLine($"[-] Eroare: Topicul '{topic}' este deja preluat de un alt terminal activ!");
                    topicMutex.Close();
                    topicMutex = null;
                }
                else
                {
                    Console.WriteLine($"[+] Ai blocat exclusiv topicul '{topic}'. Toate mesajele de aici vor merge pe el.\n");
                    break;
                }
            }

            Console.WriteLine("Tasteaza 'exit' in loc de mesaj pentru a inchide fereastra.\n");

            try
            {
                while (true)
                {
                    try
                    {
                        using (var client = new BrokerClient())
                        {
                            client.Connect(host, brokerPort, senderName);
                            Console.WriteLine($"[+] Conectat la Brokerul de pe {host}:{brokerPort}\n");

                            // 2. Bucla infinita doar pentru mesaje
                            while (true)
                            {
                                Console.Write($"Mesajul pentru '{topic}': ");
                                string content = Console.ReadLine()?.Trim();

                                if (string.IsNullOrEmpty(content)) continue;
                                if (content.ToLower() == "exit" || content.ToLower() == "quit") return;

                                var reply = client.Publish(topic, content);
                                Console.WriteLine($"  -> [Broker a confirmat]: {reply}\n");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"\n[!] Eroare de conexiune / Broker picat: {ex.Message}");
                        Console.WriteLine("[*] Reincercam reconectarea automata in 3 secunde...");
                        Thread.Sleep(3000);
                    }
                }
            }
            finally
            {
                // Cand terminalul este oprit, eliberam automat topicul pentru ceilalti
                if (topicMutex != null)
                {
                    topicMutex.ReleaseMutex();
                    topicMutex.Close();
                }
            }
        }
    }
}
