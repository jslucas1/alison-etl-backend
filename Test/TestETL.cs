using etl.Interfaces;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using etl;
using System.Dynamic;
using System.IO;
using GraphQL.Client.Http;
using Newtonsoft.Json;


namespace etl.Session
{
    public class TestETL : IEtl
    {
        private Database db;

        public TestETL()
        {
            db = new Database();
        }

        public bool ShouldRun()
        {
            return true;
        }

        public void DoWork()
        {
            Console.WriteLine("TestETL: DoWork()");
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
        }

        public void LoadLinxTable(List<ExpandoObject> linxData)
        {

        }

        public List<ExpandoObject> GetLinxData()
        {
            return new List<ExpandoObject>();
        }
    }
}