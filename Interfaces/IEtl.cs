using System.Collections.Generic;
using System.Dynamic;

namespace etl.Interfaces
{
    public interface IEtl
    {
        public void DoWork();
        public bool ShouldRun();
        public void LoadLinxTable(List<ExpandoObject> linxData);
        public List<ExpandoObject> GetLinxData();

    }
}