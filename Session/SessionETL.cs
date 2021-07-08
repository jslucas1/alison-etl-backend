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

namespace etl.Session
{
    public class SessionETL : IEtl
    {
        private int number = 0;

        public void DoWork(Config conf)
        {
            Console.WriteLine($"Run Session update logic here");
            //"select * from `alison-etl`.ETLJobPipeline where Status = \"Active\""
            
            Database db = new Database(conf);

            Console.WriteLine(db.ToString());

            List<ExpandoObject> query_res = db.Select("*", "`alison-etl`.ETLJobPipeline", "Status = \"Active\"");

            foreach (dynamic item in query_res)
            {
                Console.WriteLine(item.name);
            }
        }

        public bool ShouldRun()
        {
            number += 1;

            if (number % 5 == 0)
            {
                return true;
            }

            return false;

        }
    }
}