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
    public class SessionETL : IEtl
    {
        // is it time to run the ETL based off last run data
        // public void DoWork(Config conf)
        // {
        //     Console.WriteLine($"Run Session update logic here");
        //     //"select * from `alison-etl`.ETLJobPipeline where Status = \"Active\""

        //     Database db = new Database(conf);

        //     Console.WriteLine(db.ToString());

        //     List<ExpandoObject> query_res = db.Select("*", "`alison-etl`.ETLJobPipeline", "Status = \"Active\"");

        //     foreach (dynamic item in query_res)
        //     {
        //         Console.WriteLine(item.name);
        //     }
        // }

        public bool ShouldRun()
        {
            ConnectionString myConnection = new ConnectionString();
            string cs = myConnection.cs;
            using var con = new MySqlConnection(cs);

            string stm = "select * from `alison-etl`.ETLJobPipeline a, ";
            stm += "              `alison-etl`.ETLPipeline b";
            stm += "         where a.Status = \"Active\" AND b.Name = \"Sessions\" AND a.PipelineId = b.Id";

            List<ExpandoObject> pipelines = new List<ExpandoObject>();

            try
            {
                con.Open();
                using var cmd2 = new MySqlCommand(stm, con);
                using (var rdr = cmd2.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        Console.WriteLine("Reading a Session");
                        dynamic temp = new ExpandoObject();
                        temp.Id = rdr.GetInt32(0);
                        temp.ETLJobId = rdr.GetInt32(1);
                        temp.PipelineId = rdr.GetInt32(2);
                        temp.ScheduledMinutes = rdr.GetInt32(3);
                        temp.LastStartDate = rdr.GetString(4);
                        temp.LastStartTime = rdr.GetString(5);
                        temp.LastCompletedTime = rdr.GetString(6);
                        temp.Status = rdr.GetString(7);

                        pipelines.Add(temp);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Loading Sessions Error");
                Console.WriteLine(e.Message);
                return false;
            }
            finally
            {
                  con.Close();
            }

            foreach (dynamic pipeline in pipelines)
            {
                if (pipeline.LastCompletedTime == null)
                {
                    return false;
                }
                else
                {
                    DateTime now = DateTime.Now;
                    DateTime lastStartDate = new DateTime();
                    Console.WriteLine($"Outside IF");
                    if (pipeline.LastStartDate != null && pipeline.LastStartTime != null)
                    {
                        Console.WriteLine($"Inside IF");
                        string[] tempStartDate = pipeline.LastStartDate.Split('/');
                        int month = int.Parse(tempStartDate[0]);
                        int day = int.Parse(tempStartDate[1]);
                        int year = int.Parse(tempStartDate[2]);
                        string[] tempStartTime = pipeline.LastStartTime.Split(':');
                        int hour = int.Parse(tempStartTime[0]);
                        int minute = int.Parse(tempStartTime[1]);
                        int second = Convert.ToInt32(double.Parse(tempStartTime[2]));

                        lastStartDate = new DateTime(year, month, day, hour, minute, second);
                    }
                    TimeSpan ts = now - lastStartDate;
                    if (ts.TotalMinutes >= pipeline.ScheduledMinutes)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        // do the work for the update
        public void DoWork()
        {
            Console.WriteLine("in the session do work");
            List<Session> sessions = GetAllSessionsFromDB();
            Console.WriteLine($"{sessions.Count} Sessions Loaded");
            foreach (Session item in sessions)
            {
                Console.WriteLine($"{item.ID}, {item.Name}");
            }

            
            List<ExpandoObject> linxData = GetLinxData();

        }

        private List<ExpandoObject> GetLinxData()
        {
            //string filePath = @"C:\Users\jsluc\OneDrive\Documents\Alison\Linx-Query-Response\SessionResponse.txt";
            string filePath = @"SessionResponse.txt";
            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            List<ExpandoObject> linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        private List<Session> GetAllSessionsFromDB()
        {
            // List<ExpandoObject> sessions = new List<ExpandoObject>();
            List<Session> sessions = new List<Session>();

            ConnectionString myConnection = new ConnectionString();
            string cs = myConnection.cs;
            using var con = new MySqlConnection(cs);

            string stm = "select * from `alison`.Session Order by LinxId ASC";

            try
            {

                con.Open();
                using var cmd2 = new MySqlCommand(stm, con);


                using (var rdr = cmd2.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        // dynamic temp = new ExpandoObject();
                        // temp.Id = rdr.GetInt32(0);
                        // temp.LinxId = rdr.GetInt32(1);
                        // temp.LegislativeDays = rdr.GetInt32(2);
                        // temp.Name = rdr.GetString(3);
                        // temp.StartTime = rdr.GetString(4);
                        // temp.EndDate = rdr.GetString(5);
                        // temp.TermName = rdr.GetString(6);
                        // temp.ActiveEtlSession = rdr.GetString(7);
                        // sessions.Add(temp);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return sessions;
        }



    }
}