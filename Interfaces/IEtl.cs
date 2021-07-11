namespace etl.Interfaces
{
    public interface IEtl
    {
         public void DoWork();
         public bool ShouldRun();
    }
}