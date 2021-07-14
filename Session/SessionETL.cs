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

        public void DoWork()
        {
            Console.WriteLine("SessionETL: DoWork()");

            // Open Connection to the DB
            this.db.Open();

            // Load Data from the Linx Source 
            List<ExpandoObject> linxData = GetLinxData();
            Console.WriteLine($"{linxData.Count} Linx Sessions Loaded From API Call");

            //Delete Data in LINX table
            DeleteData("DeleteLinxSession");

            //Load Data to LINX table from API Call
            LoadLinxTable(linxData);

            //Insert new records in LINX table not in Warehouse Table
            InsertData("InsertWarehouseSession");

            //Delete records in Warehouse that is not in LINX table
            DeleteData("DeleteWarehouseSession");

            //Update records in Warehouse based on data in LINX table
            UpdateData("UpdateWarehouseSession");

            // Close Connection to the DB
            this.db.Close();
        }

        public void InsertData(string insert_proc_name)
        {
            this.db.StoredProc(insert_proc_name);
        }

        public void DeleteData(string delete_proc_name)
        {
            this.db.StoredProc(delete_proc_name);
        }

        public void UpdateData(string update_proc_name)
        {
            this.db.StoredProc(update_proc_name);
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
                this.db.Insert(stm, values);
            }
        }

        public List<ExpandoObject> GetLinxData()
        {
            List<ExpandoObject> linxData = new List<ExpandoObject>();

            // Find path of the linx data file
            // string workingDirectory = Environment.CurrentDirectory;
            // string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            string filePath = "SessionResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

    }
}