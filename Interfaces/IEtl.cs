using System.Collections.Generic;
using System.Dynamic;

namespace etl.Interfaces
{
    public interface IEtl
    {
        public void DoWork();
        public bool ShouldRun();
        public void InsertData(string insert_proc_name);
        public void DeleteData(string delete_proc_name);
        public void UpdateData(string update_proc_name);
        public void LoadLinxTable(List<ExpandoObject> linxData);
        public List<ExpandoObject> GetLinxData();
        public List<ExpandoObject> GetAllFromDB();

    }
}