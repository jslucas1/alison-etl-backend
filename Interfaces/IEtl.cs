namespace etl.Interfaces
{
    public interface IEtl
    {
         public void DoWork(Config conf);
         public bool ShouldRun();
    }
}