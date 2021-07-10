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
        private Database db;

        public void Dispose()
        {
            timer?.Dispose();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            IEtl worker = new MeetingETL();
            db = new Database();

            timer = new Timer(o =>
                {
                    if (worker.ShouldRun())
                    {
                        worker.DoWork(this.db);
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
            Console.WriteLine("Meeting Update Worker Stopped");
            return Task.CompletedTask;
        }

    }
}