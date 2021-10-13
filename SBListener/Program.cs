using Microsoft.Extensions.CommandLineUtils;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SBListener
{
    public class Program
    {
        private static CancellationTokenSource _cancelKeyPressed = new CancellationTokenSource();

        public static int Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;

            var cliApp = new CommandLineApplication(false);

            cliApp.HelpOption("-h|-?|--help");

            var endPointParam = cliApp.Option("-e | --end-point <end-point>",
                "The end point of the service bus", CommandOptionType.SingleValue);

            cliApp.Command("topic", cmd =>
            {
                cmd.Description = "Listen to a service bus topic subscription";
                cmd.HelpOption("-h|-?|--help");
                var topicArg = cmd.Argument("[topic]", "Service bus topic to listen to");
                var subscriptionArg = cmd.Argument("[subscription]", "Service bus topic subscription to listen to");

                var peekArg = cmd.Option("-p | --peek",
                    "Use Peek instead of delete when reading queue", CommandOptionType.NoValue);

                var envolpeArg = cmd.Option("-m | --message-envolpe",
                    "Write the whole message envolpe instead of just the body", CommandOptionType.NoValue);

                cmd.OnExecute(async () =>
                {
                    if (!endPointParam.HasValue() || string.IsNullOrEmpty(topicArg.Value) || string.IsNullOrEmpty(subscriptionArg.Value))
                    {
                        cmd.ShowHelp();
                        return 0;
                    }

                    var connectionString = ConnectionString(endPointParam);
                    await new SBTopicListener(connectionString, topicArg.Value, subscriptionArg.Value, peekArg.HasValue(), envolpeArg.HasValue()).Listen(_cancelKeyPressed.Token);

                    return 0;
                });
            });

            cliApp.OnExecute(() =>
            {
                cliApp.ShowHelp();
                return 0;
            });

            try
            {
                return cliApp.Execute(args);
            }
            catch (TaskCanceledException)
            {

                return 0;
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            _cancelKeyPressed.Cancel();
        }

        private static string ConnectionString(
            CommandOption endPointParam)
        {
            if (endPointParam.HasValue())
            {
                return endPointParam.Value().Trim('\'');
            }

            throw new System.NotImplementedException();
        }
    }
}
