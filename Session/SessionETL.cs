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
        private int pipelineId;
        private string dtFormat = "yyyy-MM-dd HH:mm:ss";

        public SessionETL()
        {
            db = new Database();
        }

        public bool ShouldRun()
        {
            string stm = "select * from `alison-etl`.Pipeline where name = \"Session\"";
            List<ExpandoObject> pipelines = new();

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

                Console.WriteLine($"Working on Pipeline {pipeline.Id}, {pipeline.Name}");
                pipelineId = pipeline.Id;

                if (pipeline.Status == "Inactive") // Dont work on deactivated pipelines
                {
                    return false;
                }

                if (pipeline.LastStart.Equals(DBNull.Value)) // Never run
                {

                    return true;
                }

                else if (pipeline.LastCompleted.Equals(DBNull.Value)) // currently running
                {
                    return false;
                }
                else
                {
                    DateTime now = DateTime.Now;
                    DateTime lastStart = DateTime.Parse(pipeline.LastStart);
                    TimeSpan ts = now - lastStart;
                    Console.WriteLine($"{Math.Round(ts.TotalMinutes,2)} minutes since last sync");
                    if (ts.TotalMinutes >= pipeline.ScheduledMinutes)
                    {
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
            UpdatePipelineHistory("Extract", "Complete");
            UpdatePipelineHistory("Load", "Inprocess");

            //Insert new records in LINX table not in Warehouse Table
            db.StoredProc("InsertWarehouseSession");

            //Delete records in Warehouse that is not in LINX table
            db.StoredProc("DeleteWarehouseSession");

            //Update records in Warehouse based on data in LINX table
            db.StoredProc("UpdateWarehouseSession");

            //Update History Tracking Table
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
            string workingDirectory = Environment.CurrentDirectory;
            string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            // string filePath = "SessionResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            List<ExpandoObject> linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        private void UpdatePipelineStart()
        {
            Console.WriteLine("About to Update pipeline start data");

            string stm = "UPDATE `alison-etl`.Pipeline SET LastStart = @LastStart, LastCompleted = null WHERE Id = @Id";
            string lastStart = DateTime.Now.ToString(dtFormat);

            var values = new Dictionary<string, object>() 
            { 
                {"@Id", pipelineId}, 
                {"@LastStart", lastStart} 
            };
            
            db.Update(stm, values);
        }

        private void UpdatePipelineFinish()
        {
            Console.WriteLine("About to Update pipeline Completed Time");
            string stm = "UPDATE `alison-etl`.Pipeline SET LastCompleted = @LastCompleted WHERE Id = @Id";

            string lastCompleted = DateTime.Now.ToString(dtFormat);

            var values = new Dictionary<string, object>()
            {
                {"@Id", pipelineId}, 
                {"@LastCompleted", lastCompleted}
            };
            db.Update(stm, values);
        }

        private void UpdatePipelineHistory(string step, string status)
        {
            Console.WriteLine("About to Insert " + step + " " + status + " into history");

            string stm = "INSERT INTO `alison-etl`.PipelineStatus (PipelineId, Step, Status, TimeStamp)";
            stm += "values (@PipelineId, @Step, @Status, @TimeStamp)";

            string timestamp = DateTime.Now.ToString(dtFormat);

            var values = new Dictionary<string, object>()
                {
                    {"@PipelineId", pipelineId},
                    {"@Step", step},
                    {"@Status", status},
                    {"@TimeStamp", timestamp}
                };
            db.Insert(stm, values);
        }
    }
}