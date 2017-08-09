namespace Sitecore.Support.Configuration
{
  public static class Settings
  {
    public static class LinkDatabase
    {
      public static int MaximumBatchSize =>
          Sitecore.Configuration.Settings.GetIntSetting("ClientData.MaximumBatchSize", 1000);
    }
  }
}