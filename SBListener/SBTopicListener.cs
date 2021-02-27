using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
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
        private readonly StreamWriter _stderr;

        public SBTopicListener(string connectionString, string topic, string subscription)
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

            _stderr = new StreamWriter(Console.OpenStandardError());
        }

        public async Task Listen(CancellationToken cancellationToken)
        {
            var client = new SubscriptionClient(
                _connectionString,
                _topic,
                _subscription,
                ReceiveMode.ReceiveAndDelete,
                new RetryExponential(TimeSpan.FromMilliseconds(250), TimeSpan.FromMinutes(5), 3));

            client.RegisterMessageHandler(ClientHandler, ErrorHandler);

            await Task.Delay(Timeout.Infinite, cancellationToken);

            await client.UnregisterMessageHandlerAsync(TimeSpan.FromMinutes(5));
        }

        private static Task ClientHandler(Message arg1, CancellationToken cancellationToken)
        {
            Console.WriteLine($"{Encoding.UTF8.GetString(arg1.Body)}");
            return Task.CompletedTask;
        }

        private Task ErrorHandler(ExceptionReceivedEventArgs arg)
        {
            _stderr.WriteLine($"Error: {JsonConvert.SerializeObject(arg)}");
            return Task.CompletedTask;
        }

    }
}
