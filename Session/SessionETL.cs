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
        private Database db;
        private DateTime now;
        private int jobPipelineId;

        private int pipelineId;

        public SessionETL()
        {
            db = new Database();
        }

        public bool ShouldRun()
        {
            string stm = "select * from `alison-etl`.ETLJobPipeline a, ";
            stm += "              `alison-etl`.ETLPipeline b";
            stm += "         where a.Status = \"Active\" AND b.Name = \"Sessions\" AND a.PipelineId = b.Id";

            List<ExpandoObject> pipelines = null;

            try
            {
                this.db.Open();
                pipelines = this.db.Select(stm);
                this.db.Close();
            }
            catch
            {
                Console.WriteLine("Error Pulling Pipelines Data");
                return false;
            }

            foreach (dynamic pipeline in pipelines)
            {
                if (pipeline.LastCompletedTime == null)
                {
                    return false;
                }
                else
                {
                    now = DateTime.Now;
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
                        jobPipelineId = pipeline.Id;
                        return true;
                    }
                }
            }

            return false;
        }

        public void DoWork()
        {
            Console.WriteLine("SessionETL: DoWork()");

            // Open Connection to the DB
            this.db.Open();

            //Update Tracking for start of extract
            UpdatePipelineStart();
            UpdatePipelineHistory("Extract", "Inprocess");

            // Extract Data from the Linx Source 
            List<ExpandoObject> linxData = GetLinxData();

            //Delete Data in LINX table
            db.StoredProc("DeleteLinxSession");

            //Load Data to LINX table from API Call
            LoadLinxTable(linxData);

            //Update History Tracking Table
            now = DateTime.Now;
            UpdatePipelineHistory("Extract", "Complete");
            UpdatePipelineHistory("Load", "Inprocess");

            //Insert new records in LINX table not in Warehouse Table
            db.StoredProc("InsertWarehouseSession");

            //Delete records in Warehouse that is not in LINX table
            db.StoredProc("DeleteWarehouseSession");

            //Update records in Warehouse based on data in LINX table
            db.StoredProc("UpdateWarehouseSession");

            //Update History Tracking Table
            now = DateTime.Now;
            UpdatePipelineHistory("Load", "Complete");
            UpdatePipelineFinish();

            // Close Connection to the DB
            this.db.Close();
        }

        public void LoadLinxTable(List<ExpandoObject> linxData)
        {
            Console.WriteLine("About to load the linx data");

            string stm = "INSERT INTO `alison-etl`.LINXSession";
            stm += "             (LinxId, LegislativeDays, Name, StartDate, EndDate, TermName)";
            stm += "      VALUES (@LinxId, @LegislativeDays, @Name, @StartDate, @EndDate, @TermName)";


            foreach (dynamic item in linxData)
            {
                var values = new Dictionary<string, object>()
                {
                    {"@LinxId", item.id},
                    {"@LegislativeDays", item.legislativeDays},
                    {"@Name", item.name},
                    {"@StartDate", item.startDate},
                    {"@EndDate", item.endDate},
                    {"@TermName", item.term.name},
                };
                db.Insert(stm, values);
            }
        }

        // TODO Update with API source
        public List<ExpandoObject> GetLinxData()
        {
            // ApiManager api = new ApiManager();

            // Find path of the linx data file
            // string workingDirectory = Environment.CurrentDirectory;
            // string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            string filePath = "SessionResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            List<ExpandoObject> linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        private void UpdatePipelineStart()
        {

            Console.WriteLine("About to Update pipeline start data");

            string stm = "UPDATE `alison-etl`.ETLJobPipeline ";
            stm += "SET LastStartDate = @LastStartDate, LastStartTime = @LastStartTime ";
            stm += "WHERE PipelineId = @PipelineId AND Status = 'Active'";
            string lastStartDate = now.Month + "/" + now.Day + "/" + now.Year;
            string lastStartTime = now.Hour + ":" + now.Minute + ":" + now.Second;

            var values = new Dictionary<string, object>()
                {
                    {"@PipelineId", pipelineId},
                    {"@LastStartDate", lastStartDate},
                    {"@LastStartTime", lastStartTime},

                };
            db.Update(stm, values);
        }

        private void UpdatePipelineFinish()
        {

            Console.WriteLine("About to Update pipeline Completed Time");

            string stm = "UPDATE `alison-etl`.ETLJobPipeline ";
            stm += "SET LastCompletedTime = @LastCompletedTime ";
            stm += "WHERE PipelineId = @PipelineId AND Status = 'Active'";
            string lastCompletedTime = now.Hour + ":" + now.Minute + ":" + now.Second;

            var values = new Dictionary<string, object>()
                {
                    {"@PipelineId", pipelineId},
                    {"@LastCompletedTime", lastCompletedTime},

                };
            db.Update(stm, values);
        }
        private void UpdatePipelineHistory(string step, string status)
        {

            Console.WriteLine("About to Insert " + step + " " + status + " into history");

            string stm = "INSERT INTO `alison-etl`.ETLJobPipelineStatus ";
            stm += "(ETLJobPipelineId, Step, Status, Date, TimeStamp) ";
            stm += "values (@JobPipelineId, @Step, '@Status', @StartDate, @StartTime)";
            string date = now.Month + "/" + now.Day + "/" + now.Year;
            string time = now.Hour + ":" + now.Minute + ":" + now.Second;

            var values = new Dictionary<string, object>()
                {
                    {"@JobPipelineId", jobPipelineId},
                    {"@Step", step},
                    {"@Status", status},
                    {"@StartDate", date},
                    {"@StartTime", time},

                };
            db.Insert(stm, values);

        }

    }
}