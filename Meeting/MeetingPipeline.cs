using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using etl.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace etl.Meeting
{
    public class MeetingPipeline : IPipeline
    {
        private Timer timer;

        public void Dispose()
        {
            timer?.Dispose();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Config conf = new Config();
            IEtl worker = new MeetingETL();

            timer = new Timer(o =>
                {
                    if (worker.ShouldRun())
                    {
                        //worker.DoWork(conf);
                        worker.DoWork();
                    }
                    else
                    {
                        Console.WriteLine("Not time to work on Meeting");
                    }
                },
                null,
                TimeSpan.Zero,
                TimeSpan.FromSeconds(10)
            );
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Printing worker stopped");
            return Task.CompletedTask;
        }

    }
}