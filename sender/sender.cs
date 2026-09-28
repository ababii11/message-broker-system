using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace MessageBroker.Sender
{
    public class LoginMessage
    {
        public string type { get; set; } = "login";
        public string user { get; set; }
    }

    public class PublishMessage
    {
        public string type { get; set; } = "publish";
        public string sender { get; set; }
        public string topic { get; set; }
        public string content { get; set; }
        public string timestamp { get; set; }
    }

    public class BrokerClient : IDisposable
    {
        private readonly TcpClient _client;
        private StreamReader _reader;
        private StreamWriter _writer;
        private string _senderName;

        public BrokerClient(int localPort)
        {
            // Asignăm un port local diferit și unic
            IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Loopback, localPort);
            _client = new TcpClient(localEndPoint);
        }

        public void Connect(string host, int port, string senderName)
        {
            _senderName = senderName;
            _client.Connect(host, port);
            
            var stream = _client.GetStream();
            _reader = new StreamReader(stream);
            _writer = new StreamWriter(stream) { AutoFlush = true };

            var loginMsg = new LoginMessage { user = senderName };
            _writer.WriteLine(JsonSerializer.Serialize(loginMsg));
            
            string response = _reader.ReadLine();
            if (response == null || !response.Contains("\"login_ok\""))
                throw new IOException($"Brokerul a respins inregistrarea: {response}");
        }

        public string Publish(string topic, string content)
        {
            var message = new PublishMessage 
            { 
                sender = _senderName,
                topic = topic.Trim(), 
                content = content.Trim(),
                timestamp = DateTime.UtcNow.ToString("O")
            };
            _writer.WriteLine(JsonSerializer.Serialize(message));
            return _reader.ReadLine();
        }

        public string GetLocalEndpoint() => _client.Client.LocalEndPoint.ToString();

        public void Dispose()
        {
            _reader?.Dispose();
            _writer?.Dispose();
            _client?.Dispose();
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Daca argumentul "child" exista, suntem intr-una din ferestrele spawnate
            if (args.Length > 0 && args[0] == "child")
            {
                int senderId = int.Parse(args[1]);
                int localPort = int.Parse(args[2]);
                RunInteractiveChild(senderId, localPort);
            }
            else
            {
                // Daca nu avem argumente, suntem in programul "Master" care creeaza terminalele
                RunSpawner();
            }
        }

        static void RunSpawner()
        {
            Console.WriteLine("=== MASTER SENDER ===");
            Console.Write("Introdu X (numarul de terminale cu senderi pe care doriti sa le deschideti): ");
            if (!int.TryParse(Console.ReadLine(), out int x) || x <= 0)
            {
                Console.WriteLine("Numar invalid. Se va folosi X = 2.");
                x = 2;
            }

            // Identificam cum a fost pornit programul (.exe direct sau prin dotnet run)
            string exePath = Process.GetCurrentProcess().MainModule.FileName;
            string dllPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            bool isDotNet = exePath.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase);

            for (int i = 1; i <= x; i++)
            {
                int port = 51000 + i;
                ProcessStartInfo psi = new ProcessStartInfo();
                
                if (isDotNet)
                {
                    psi.FileName = exePath;
                    psi.Arguments = $"\"{dllPath}\" child {i} {port}";
                }
                else
                {
                    psi.FileName = exePath;
                    psi.Arguments = $"child {i} {port}";
                }
                
                // Setarea magica pe Windows: deschide fizic o fereastra CMD noua pentru fiecare proces
                psi.UseShellExecute = true; 
                
                try
                {
                    Process.Start(psi);
                }
                catch(Exception e)
                {
                    Console.WriteLine($"Eroare la deschiderea terminalului {i}: {e.Message}");
                }
            }

            Console.WriteLine($"\n[INFO] Au fost deschise {x} ferestre noi separate.");
            Console.WriteLine("Poti minimiza sau inchide aceasta fereastra principala. Mergi la ferestrele nou deschise pentru a scrie mesaje.");
            Console.ReadLine();
        }

        static void RunInteractiveChild(int senderId, int localPort)
        {
            string host = "127.0.0.1";
            int brokerPort = 9000;
            string senderName = $"Sender-{senderId}";

            Console.Title = $"Terminal - {senderName} (Port local: {localPort})";
            Console.WriteLine(new string('=', 50));
            Console.WriteLine($" TERMINAL SENDER INTERACTIV | {senderName} ");
            Console.WriteLine(new string('=', 50));

            try
            {
                using (var client = new BrokerClient(localPort))
                {
                    client.Connect(host, brokerPort, senderName);
                    Console.WriteLine($"[+] Conectat la Brokerul de pe {host}:{brokerPort}");
                    Console.WriteLine($"[+] Portul Meu Local: {client.GetLocalEndpoint()}\n");

                    while (true)
                    {
                        Console.Write("Topic-ul (ex: sport, weather, music, etc.): ");
                        string topic = Console.ReadLine()?.Trim();
                        
                        if (string.IsNullOrEmpty(topic)) continue;

                        Console.Write($"Mesajul de transmis catre topicul '{topic}': ");
                        string content = Console.ReadLine()?.Trim();

                        if (string.IsNullOrEmpty(content)) continue;

                        try
                        {
                            string response = client.Publish(topic, content);
                            Console.WriteLine($"  -> [Broker a confirmat]: {response}\n");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  -> [Eroare la trimitere]: {ex.Message}");
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[!] Eroare de conexiune: {ex.Message}");
                Console.WriteLine("Ai pornit broker.py in prealabil?");
            }
            
            Console.WriteLine("\nApasa orice tasta pentru a inchide terminalul...");
            Console.ReadKey();
        }
    }
}
