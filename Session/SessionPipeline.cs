using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using etl.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace etl.Session
{
    public class SessionPipeline : IPipeline
    {
        private Timer timer;
        private Database db;

        public void Dispose()
        {
            timer?.Dispose();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            IEtl worker = new SessionETL();
            db = new Database();

            timer = new Timer(o =>
                {
                    if (worker.ShouldRun())
                    {
                        worker.DoWork(this.db);
                    }
                    else
                    {
                        Console.WriteLine("Not time to work on Session");
                    }
                },
                null,
                TimeSpan.Zero,
                TimeSpan.FromSeconds(15)
            );
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Session Update Worker Stopped");
            return Task.CompletedTask;
        }

    }
}