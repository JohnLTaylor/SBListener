using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SBListener
{
    public class SBTopicListener
    {
        private readonly string _connectionString;
        private readonly string _topic;
        private readonly string _subscription;
        private readonly bool _peek;
        private readonly bool _writeMessageEnvolpe;
        private readonly StreamWriter _stdout;
        private readonly StreamWriter _stderr;

        public SBTopicListener(string connectionString, string topic, string subscription, bool peek, bool writeMessageEnvolpe)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new System.ArgumentException($"'{nameof(connectionString)}' cannot be null or whitespace.", nameof(connectionString));
            }

            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new System.ArgumentException($"'{nameof(topic)}' cannot be null or whitespace.", nameof(topic));
            }

            _connectionString = connectionString;
            _topic = topic;
            _subscription = subscription;
            _peek = peek;
            _writeMessageEnvolpe = writeMessageEnvolpe;

            _stdout = new StreamWriter(Console.OpenStandardOutput(8192));
            _stderr = new StreamWriter(Console.OpenStandardError(8192));
        }

        public async Task Listen(CancellationToken cancellationToken)
        {
            var client = new SubscriptionClient(
                _connectionString,
                _topic,
                _subscription,
                _peek ? ReceiveMode.PeekLock : ReceiveMode.ReceiveAndDelete,
                new RetryExponential(TimeSpan.FromMilliseconds(250), TimeSpan.FromMinutes(5), 3));

            client.RegisterMessageHandler(ClientHandler, ErrorHandler);

            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (TaskCanceledException)
            {
            }
            await client.UnregisterMessageHandlerAsync(TimeSpan.FromMinutes(5));
        }

        private Task ClientHandler(Message arg1, CancellationToken cancellationToken)
        {
            if (_writeMessageEnvolpe)
            {
                var jObject = JObject.FromObject(arg1);
                jObject["Body"] = JToken.Parse(Encoding.UTF8.GetString(arg1.Body));
                _stdout.WriteLine(jObject);
            }
            else
            {
                _stdout.WriteLine($"{Encoding.UTF8.GetString(arg1.Body)}");
            }
            _stdout.Flush();
            return Task.CompletedTask;
        }

        private Task ErrorHandler(ExceptionReceivedEventArgs arg)
        {
            _stderr.WriteLine($"Error: {JsonConvert.SerializeObject(arg)}");
            return Task.CompletedTask;
        }

    }
}
