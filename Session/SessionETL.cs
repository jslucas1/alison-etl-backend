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
                    if (pipeline.LastStartDate != null && pipeline.LastStartTime != null)
                    {
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

        // is it time to run the ETL based off last run data
        // TODO: work from the config > public void DoWork(Config conf)
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
            LoadLinxTable(linxData);
        }

        private void InsertData()
        {
            //add sql to insert records that don't exist
            List<Session> inserts = new List<Session>();
        }

        private void DeleteData()
        {
            //add sql to delete records that no longer exist
            List<Session> deletes = new List<Session>();
        }

        private void UpdateData()
        {
            //add sql to update records that have changed
            List<Session> changes = new List<Session>();

        }

        private void LoadLinxTable(List<ExpandoObject> linxData)
        {
            Console.WriteLine("About to load the linx data");
            ConnectionString myConnection = new ConnectionString();
            string cs = myConnection.cs;
            using var con = new MySqlConnection(cs);

            string stm = "INSERT INTO `alison-etl`.LINXSession";
            stm += "             (LinxId, LegislativeDays, Name, StartDate, EndDate, TermName)";
            stm += "      VALUES (@LinxId, @LegislativeDays, @Name, @StartDate, @EndDate, @TermName)";

            string delStm = "DELETE FROM `alison-etl`.LINXSession";

            try
            {
                con.Open();

                using var delCmd = new MySqlCommand(delStm, con);
                foreach (dynamic item in linxData)
                {
                    using var cmd = new MySqlCommand(stm, con);
                    cmd.Parameters.AddWithValue("@LinxId", item.id);
                    cmd.Parameters.AddWithValue("@LegislativeDays", item.legislativeDays);
                    cmd.Parameters.AddWithValue("@Name", item.name);
                    cmd.Parameters.AddWithValue("@StartDate", item.startDate);
                    cmd.Parameters.AddWithValue("@EndDate", item.endDate);
                    cmd.Parameters.AddWithValue("@TermName", item.term.name);

                    cmd.Prepare();
                    cmd.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Loading LINX Sessions Error");
                Console.WriteLine(e.Message);
            }
            finally
            {
                Console.WriteLine("Just Finished Loading LINX Data");
                con.Close();
            }
        }

        private List<ExpandoObject> GetLinxData()
        {
            List<ExpandoObject> linxData = new List<ExpandoObject>();

            //string filePath = @"C:\Users\jsluc\OneDrive\Documents\Alison\Linx-Query-Response\SessionResponse.txt";

            // Find path of the linx data file
            string workingDirectory = Environment.CurrentDirectory;
            string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        private List<Session> GetAllSessionsFromDB()
        {
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
                        sessions.Add(new Session()
                        {
                            ID = rdr.GetInt32(0),
                            LinxId = rdr.GetInt32(1),
                            LegislativeDays = rdr.GetInt32(2),
                            Name = rdr.GetString(3),
                            StartTime = rdr.GetString(4),
                            EndDate = rdr.GetString(5),
                            TermName = rdr.GetString(6),
                            ActiveEtlSession = rdr.GetString(7)
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                con.Close();
            }
            return sessions;
        }
    }
}