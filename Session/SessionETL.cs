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

        public void DoWork()
        {
            Console.WriteLine($"Run Session update logic here");
        }

        public bool ShouldRun()
        {
            number += 1;

            if (number % 5 == 0)
            {
                return true;
            }

            return false;



            // ConnectionString myConnection = new ConnectionString();
            // string cs = myConnection.cs;
            // using var con = new MySqlConnection(cs);

            // string stm = "select * from	`alison-etl`.ETLJobPipeline where Status = \"Active\"";

            // List<ExpandoObject> pipelines = new List<ExpandoObject>();

            // try{

            // con.Open();
            // using var cmd2 = new MySqlCommand(stm, con);


            // using (var rdr = cmd2.ExecuteReader())
            // {
            //     while(rdr.Read())
            //     {
            //         dynamic temp = new ExpandoObject();
            //         temp.Id = rdr.GetInt32(0);
            //         temp.ETLJobId = rdr.GetInt32(1);
            //         temp.PipelineId = rdr.GetInt32(2);
            //         temp.ScheduledMinutes = rdr.GetInt32(3);
            //         temp.LastStartDate = rdr.GetString(4);
            //         temp.LastStartTime = rdr.GetString(5);
            //         temp.Status = rdr.GetString(6);
            //         pipelines.Add(temp);
            //     }
            // }
            // } catch(Exception e){
            //     Console.WriteLine(e.Message);
            // }
        }
    }
}