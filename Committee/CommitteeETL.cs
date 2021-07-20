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


namespace etl.Committee
{
    public class CommitteeETL : IEtl
    {
        private Database db;
        private int pipelineId;
        private string dtFormat = "yyyy-MM-dd HH:mm:ss";

        public CommitteeETL()
        {
            db = new Database();
        }

        public bool ShouldRun()
        {
            string stm = "select * from `alison-etl`.Pipeline where name = \"Committee\"";
            List<ExpandoObject> pipelines = new();

            try
            {
                this.db.Open();
                pipelines = this.db.Select(stm);
                this.db.Close();
            }
            catch
            {
                return false;
            }

            foreach (dynamic pipeline in pipelines)
            {
                pipelineId = pipeline.Id;

                if (pipeline.Status != "Active") // Dont run inactives pipelines
                {
                    return false;
                }

                if (pipeline.LastStart.Equals(DBNull.Value) || pipeline.LastStart.Equals("")) // Never run
                {
                    return true;
                }
                else if (pipeline.LastCompleted.Equals(DBNull.Value) || pipeline.LastCompleted.Equals("")) // currently running
                {
                    return false;
                }
                else // Do the thing
                {
                    DateTime now = DateTime.Now;
                    DateTime lastStart = DateTime.Parse(pipeline.LastStart);
                    TimeSpan ts = now - lastStart;
                    if (ts.TotalMinutes >= pipeline.ScheduledMinutes) // Has it been enough time? 
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public void DoWork()
        {
            this.db.Open(); // Open Connection to the DB

            UpdatePipelineStart(); // Update Tracking for start

            UpdatePipelineHistory("Extract", "Inprocess"); // Update History Tracking Table

            List<ExpandoObject> linxData = GetLinxData(); // Extract Data from the Linx Source 

            db.StoredProc("DeleteLinxCommittee"); // Delete Data in LINX table

            LoadLinxTable(linxData); // Load Data to LINX table from API Call

            UpdatePipelineHistory("Extract", "Complete"); // Update History Tracking Table

            UpdatePipelineHistory("Load", "Inprocess"); // Update History Tracking Table

            db.StoredProc("InsertWarehouseCommittee"); // Insert new records in LINX table not in Warehouse Table

            db.StoredProc("DeleteWarehouseCommittee"); // Delete records in Warehouse that is not in LINX table

            db.StoredProc("UpdateWarehouseCommittee"); // Update records in Warehouse based on data in LINX table

            UpdatePipelineHistory("Load", "Complete"); // Update History Tracking Table

            UpdatePipelineFinish(); // Update Tracking for finish

            this.db.Close(); // Close Connection to the DB
        }

        public void LoadLinxTable(List<ExpandoObject> linxData)
        {
            string stm = "INSERT INTO `alison-etl`.LINXCommittee";
            stm += "             (LinxId, Chamber, Name, Type, ClerkName, ClerkEmail)";
            stm += "      VALUES (@LinxId, @Chamber, @Name, @Type, @ClerkName, @ClerkEmail)";


            foreach (dynamic item in linxData)
            {
                var values = new Dictionary<string, object>()
                {
                    {"@LinxId", item.id},
                    {"@Chamber", item.chamber},
                    {"@Name", item.name},
                    {"@Type", item.type},
                    {"@ClerkName", item.clerk.displayName},
                    {"@ClerkEmail", item.clerk.email},
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
            //string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            string filePath = "Committees.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            List<ExpandoObject> linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        private void UpdatePipelineStart()
        {
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
            string stm = "INSERT INTO `alison-etl`.PipelineStatus (PipelineId, Step, Status, TimeStamp)" +
                         " values (@PipelineId, @Step, @Status, @TimeStamp)";
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