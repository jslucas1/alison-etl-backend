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

        /// <summary>
        /// SessionETL Construtor
        /// </summary>
        public SessionETL()
        {
            db = new Database();
        }

        /// <summary>
        /// This method performs the calculation to run the sync or not
        /// </summary>
        /// <returns>whether or not to run bool</returns>
        public bool ShouldRun()
        {
            string stm = "select * from `alison-etl`.ETLJobPipeline a, ";
            stm += "              `alison-etl`.ETLPipeline b";
            stm += "         where a.Status = \"Active\" AND b.Name = \"Sessions\" AND a.PipelineId = b.Id";

            List<ExpandoObject> pipelines = this.db.Select(stm);

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

        /// <summary>
        /// This method is the driver for the work performed.
        /// </summary>
        public void DoWork()
        {
            Console.WriteLine("SessionETL: DoWork()");
            List<ExpandoObject> sessions = GetAllFromDB();
            Console.WriteLine($"{sessions.Count} Sessions Loaded From DB");

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
        }

        /// <summary>
        /// add sql to insert records that don't exist
        /// </summary>
        private void InsertData(string insert_proc_name)
        {
            //add sql to insert records that don't exist
            this.db.StoredProc(insert_proc_name);
        }

        /// <summary>
        /// add sql to delete records that no longer exist
        /// </summary>
        private void DeleteData(string delete_proc_name)
        {
            this.db.StoredProc(delete_proc_name);
        }

        /// <summary>
        /// add sql to update records that have changed
        /// </summary>
        private void UpdateData(string update_proc_name)
        {
            this.db.StoredProc(update_proc_name);
        }

        /// <summary>
        /// This method loads the provided raw linx session data
        /// into the temp LINXSession table
        /// </summary>
        /// <param name="linxData"><c>List<ExpandoObject></c></param>
        private void LoadLinxTable(List<ExpandoObject> linxData)
        {
            Console.WriteLine("About to load the linx data");

            string stm = "INSERT INTO `alison-etl`.LINXSession";
            stm += "             (LinxId, LegislativeDays, Name, StartDate, EndDate, TermName)";
            stm += "      VALUES (@LinxId, @LegislativeDays, @Name, @StartDate, @EndDate, @TermName)";


            foreach (dynamic item in linxData)
            {
                Dictionary<string, string> values = new Dictionary<string, string>()
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

        /// <summary> This method reads linx data into a List from a file
        /// TODO: Modification needed after api becomes openly avaliable.
        /// <example> For example:
        /// <code>List<ExpandoObject> linxData = GetLinxData()</code>
        /// </example>
        /// </summary>
        /// <returns><c>List<ExpandoObject></c></returns>
        private List<ExpandoObject> GetLinxData()
        {
            List<ExpandoObject> linxData = new List<ExpandoObject>();

            // Find path of the linx data file
            string workingDirectory = Environment.CurrentDirectory;
            //string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            string filePath = "SessionResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            linxData = JsonConvert.DeserializeObject<List<ExpandoObject>>(json);

            return linxData;
        }

        /// <summary>
        /// This method performs a database query for all session data 
        /// ordered by LinxId ASC
        /// <example> For example:
        /// <code>List<ExpandoObject> sessions = GetAllFromDB()</code>
        /// </example>
        /// </summary>
        /// <returns><c>List<ExpandoObject></c></returns>
        private List<ExpandoObject> GetAllFromDB()
        {
            string stm = "select * from `alison`.Session Order by LinxId ASC";
            List<ExpandoObject> sessions = this.db.Select(stm);

            return sessions;
        }
    }
}