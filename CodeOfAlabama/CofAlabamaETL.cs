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


namespace etl.CodeOfAlabama
{
    public class CofAlabamaETL : IEtl
    {
        private Database db;
        private int pipelineId;
        private string dtFormat = "yyyy-MM-dd HH:mm:ss";
        private string linxJson;

        public CofAlabamaETL()
        {
            db = new Database();
        }

        public bool ShouldRun()
        {
            string stm = "select * from `alison-etl`.Pipeline where Name = \"CodeOfAlabama\"";
            List<ExpandoObject> pipelines = new();

            try
            {
                this.db.Open();
                pipelines = this.db.Select(stm);
                this.db.Close();
                Console.WriteLine("Closed the database");
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message );
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

            db.StoredProc("DeleteLinxCodeOfAlabama"); // Delete Data in LINX table

            LoadLinxTable(linxData); // Load Data to LINX table from API Call

            UpdatePipelineHistory("Extract", "Complete"); // Update History Tracking Table

            UpdatePipelineHistory("Load", "Inprocess"); // Update History Tracking Table

            db.StoredProc("InsertWarehouseCofAlabama"); // Insert new records in LINX table not in Warehouse Table

            //Can never delete multilayer data.  
            //db.StoredProc("DeleteWarehouseCofAlabama"); // Delete records in Warehouse that is not in LINX table

            db.StoredProc("UpdateWarehouseCofAlabama"); // Update records in Warehouse based on data in LINX table

            UpdatePipelineHistory("Load", "Complete"); // Update History Tracking Table

            UpdatePipelineFinish(); // Update Tracking for finish

            this.db.Close(); // Close Connection to the DB
        }

        public void LoadLinxTable(List<ExpandoObject> linxData)
        {
            //The following is old and will be deleted once the new version is tested
            // string stm = "INSERT INTO `alison-etl`.LINXCodeOfAlabama";
            // stm += "      (LinxId, TitleId, TitleName, TitleDescription, TitleSortOrder, ";
            // stm += "      ChapterId, ChapterName, ChapterSortOrder, ChapterDescription, ";
            // stm += "      SectionId, SectionDisplayId, SectionName, SectionSortOrder, SectionDescription, ";
            // stm += "      ContentId, ContentParagraph, ContentSortOrder)";
            // stm += "      VALUES (@LinxId, @TitleId, @TitleName, @TitleDescription, @TitleSortOrder, ";
            // stm += "      @ChapterId, @ChapterName, @ChapterSortOrder, @ChapterDescription, ";
            // stm += "      @SectionId, @SectionDisplayId, @SectionName, @SectionSortOrder, @SectionDescription, ";
            // stm += "      @ContentId, @ContentParagraph, @ContentSortOrder)";

            // linxData = DenormalizeData(linxData);

            // foreach (dynamic item in linxData)
            // {
            //     var values = new Dictionary<string, object>()
            //     {
            //         {"@LinxId", item.id},
            //         {"@TitleId", item.titleId},
            //         {"@TitleName", item.titleName},
            //         {"@TitleDescription", item.titleDescription},
            //         {"@TitleSortOrder", item.titleSortOrder},
            //         {"@ChapterId", item.chapterId},
            //         {"@ChapterName", item.chapterName},
            //         {"@ChapterSortOrder", item.chapterSortOrder},
            //         {"@ChapterDescription", item.chapterDescription},
            //         {"@SectionId", item.sectionId},
            //         {"@SectionDisplayId", item.sectionDisplayId},
            //         {"@SectionName", item.sectionName},
            //         {"@SectionSortOrder", item.sectionSortOrder},
            //         {"@SectionDescription", item.sectionDescription},
            //         {"@ContentId", item.contentId},
            //         {"@ContentParagraph", item.contentParagraph},
            //         {"@ContentSortOrder", item.contentSortOrder}
            //     };
            //     db.Insert(stm, values);
            // }

            // New Multilayer Data Approach
            string stm = "INSERT INTO `alison-etl`.LINXMultiLayerData";
            stm += "      (LinxId, JsonData)";
            stm += "      VALUES (@LinxId, @JsonData)";

            Console.WriteLine(linxJson);

            var values = new Dictionary<string, object>()
                 {
                     {"@LinxId", "CodeOfAlabama"},
                     {"@JsonData", linxJson}
                 };
            db.Insert(stm, values);
        }

        private List<ExpandoObject> DenormalizeData(List<ExpandoObject> linxData)
        {
            List<ExpandoObject> returnList = new List<ExpandoObject>();

            foreach (dynamic titleLevel in linxData)
            {
                foreach (dynamic chapterLevel in titleLevel.chapters)
                {
                    foreach (dynamic sectionLevel in chapterLevel.sections)
                    {
                        foreach (dynamic contentLevel in sectionLevel.content)
                        {
                            dynamic tempObject = new ExpandoObject();

                            tempObject.titleId = titleLevel.id;
                            tempObject.titleName = titleLevel.name;
                            tempObject.titleDescription = titleLevel.description;
                            tempObject.titleSortOrder = titleLevel.sortOrder;

                            tempObject.chapterId = chapterLevel.id;
                            tempObject.chapterName = chapterLevel.name;
                            tempObject.chapterSortOrder = chapterLevel.sortOrder;
                            tempObject.chapterDescription = chapterLevel.description;

                            tempObject.sectionId = sectionLevel.id;
                            tempObject.sectionDisplayId = sectionLevel.displayId;
                            tempObject.sectionName = sectionLevel.name;
                            tempObject.sectionSortOrder = sectionLevel.sortOrder;
                            tempObject.sectionDescription = sectionLevel.description;

                            tempObject.contentId = contentLevel.id;
                            tempObject.contentParagraph = contentLevel.paragraph;
                            tempObject.contentSortOrder = contentLevel.sortOrder;
                            tempObject.id = $"T{tempObject.titleId}C{tempObject.chapterId}S{tempObject.sectionId}C{tempObject.contentId}";

                            returnList.Add(tempObject);
                        }

                    }
                }


            }

            return returnList;
        }

        // TODO Update with API source
        public List<ExpandoObject> GetLinxData()
        {
            // ApiManager api = new ApiManager();

            // Find path of the linx data file
            string workingDirectory = Environment.CurrentDirectory;
            //string filePath = $"{Directory.GetParent(workingDirectory).Parent.Parent.FullName}/SessionResponse.txt";
            string filePath = "codeOfAlabamaResponse.txt";


            StreamReader inFile = new StreamReader(filePath);
            string json = inFile.ReadToEnd();
            linxJson = json;

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