namespace etl.Interfaces
{
    public interface IEtl
    {
         public void DoWork(Database db);
         public bool ShouldRun();
    }
}