using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using etl.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace etl.Member
{
    public class MemberPipeline : IPipeline
    {
        private readonly string pipelineName = "Member";
        private int checkInSeconds = 60;
        private Timer timer;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            IEtl worker = new MemberETL();

            timer = new Timer(o =>
                {
                    if (worker.ShouldRun())
                    {
                        worker.DoWork();
                    }
                    else
                    {
                        Console.WriteLine($"Not time to work on {pipelineName}");
                    }
                },
                null,
                TimeSpan.Zero,
                TimeSpan.FromSeconds(checkInSeconds)
            );
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"{pipelineName} Update Worker Stopped");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            timer?.Dispose();
        }

    }
}