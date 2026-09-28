using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace MessageBroker.Sender
{
    // Modelele pentru serializarea JSON automata
    public class RegisterMessage
    {
        public string type { get; set; } = "REGISTER_SENDER";
        public string name { get; set; }
    }

    public class PublishMessage
    {
        public string type { get; set; } = "PUBLISH";
        public string id { get; set; }
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

        public BrokerClient(string localIp, int localPort)
        {
            // Setam endpoint-ul local (adresa si portul unic pentru aceasta instanta)
            IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Parse(localIp), localPort);
            _client = new TcpClient(localEndPoint);
        }

        public void Connect(string host, int port, string senderName)
        {
            _client.Connect(host, port);
            
            var stream = _client.GetStream();
            _reader = new StreamReader(stream);
            _writer = new StreamWriter(stream) { AutoFlush = true };

            // Inregistrarea sender-ului la Broker
            var regMsg = new RegisterMessage { name = senderName };
            string jsonReg = JsonSerializer.Serialize(regMsg);
            
            _writer.WriteLine(jsonReg);
            
            string response = _reader.ReadLine();
            if (response == null || (!response.Contains("\"status\": \"REGISTERED\"") && !response.Contains("\"status\":\"REGISTERED\"")))
            {
                throw new IOException($"Brokerul a respins inregistrarea: {response}");
            }
        }

        public string Publish(string senderName, string topic, string content)
        {
            // Cream mesajul exact cu structura din Java
            var message = new PublishMessage
            {
                id = Guid.NewGuid().ToString(),
                sender = senderName,
                topic = topic.Trim().ToLower(),
                content = content.Trim(),
                timestamp = DateTime.UtcNow.ToString("O") // Format ISO 8601
            };

            string jsonPublish = JsonSerializer.Serialize(message);
            _writer.WriteLine(jsonPublish);

            // Asteptam raspunsul Brokerului
            string response = _reader.ReadLine();
            if (response == null)
            {
                throw new IOException("Brokerul a inchis conexiunea.");
            }
            return response;
        }

        public string GetLocalEndpoint()
        {
            return _client.Client.LocalEndPoint.ToString();
        }

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
            string host = "127.0.0.1";
            int port = 5000;

            Console.Write("Sender name: ");
            string senderName = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(senderName)) senderName = "Sender";

            Console.Write("Local IP (unique per instance) [127.0.1.1]: ");
            string localIp = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(localIp)) localIp = "127.0.1.1";

            Console.Write("Local port (unique per instance) [51001]: ");
            string localPortText = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(localPortText)) localPortText = "51001";
            int localPort = int.Parse(localPortText);

            Console.WriteLine(new string('=', 48));
            Console.WriteLine($" PAD SENDER - C# | {senderName}");
            Console.WriteLine(new string('=', 48));

            try
            {
                using (var client = new BrokerClient(localIp, localPort))
                {
                    client.Connect(host, port, senderName);
                    Console.WriteLine($"Local endpoint: {client.GetLocalEndpoint()}");
                    Console.WriteLine($"Connected to Broker at {host}:{port}");

                    RunMenu(client, senderName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Eroare de conexiune: {ex.Message}");
                Console.WriteLine("Verifica daca Brokerul este pornit.");
            }
        }

        static void RunMenu(BrokerClient client, string senderName)
        {
            while (true)
            {
                Console.WriteLine("\n1. Send message");
                Console.WriteLine("2. Exit");
                Console.Write("Choice: ");
                string choice = Console.ReadLine()?.Trim();

                if (choice == "2")
                {
                    Console.WriteLine("Sender stopped.");
                    break;
                }
                if (choice != "1")
                {
                    Console.WriteLine("Invalid choice.");
                    continue;
                }

                Console.Write("Topic: ");
                string topic = Console.ReadLine()?.Trim();
                Console.Write("Message: ");
                string content = Console.ReadLine()?.Trim();

                if (string.IsNullOrEmpty(topic) || string.IsNullOrEmpty(content))
                {
                    Console.WriteLine("Topic si mesaj nu pot fi goale.");
                    continue;
                }

                try
                {
                    string response = client.Publish(senderName, topic, content);
                    Console.WriteLine($"Broker response: {response}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Eroare la trimitere: {ex.Message}");
                    break;
                }
            }
        }
    }
}