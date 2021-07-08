namespace etl.Interfaces
{
    public interface IEtl
    {
         //public void DoWork(Config conf);
         public void DoWork();
         public bool ShouldRun();
    }
}