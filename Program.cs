using System.Net.Security;
using etl.Session;
using etl.Committee;
using etl.Member;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace etl
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                    services.AddHostedService<SessionPipeline>().
                    AddHostedService<CommitteePipeline>().
                    AddHostedService<MemberPipeline>()
                );
    }
}
