using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace etl.Interfaces
{
    public interface IPipeline : IHostedService, IDisposable { }
}