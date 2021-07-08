using System;
using System.Collections.Generic;
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

        public void Dispose()
        {
            timer?.Dispose();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            IEtl worker = new SessionETL();

            timer = new Timer(o =>
                {
                    if (worker.ShouldRun())
                    {
                        worker.DoWork();
                    }
                    else
                    {
                        Console.WriteLine("Not time to work on Session");
                    }
                },
                null,
                TimeSpan.Zero, 
                TimeSpan.FromSeconds(2)
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