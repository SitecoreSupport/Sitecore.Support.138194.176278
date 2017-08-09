using Sitecore.Caching;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics.PerformanceCounters;
using System.Reflection;

namespace Sitecore.Support.Data.SqlServer
{
  public class SqlServerClientDataStore : Sitecore.Data.SqlServer.SqlServerClientDataStore
  {
    public SqlServerClientDataStore(SqlDataApi api, string objectLifetime) : base(api, objectLifetime)
    {
    }

    public SqlServerClientDataStore(string connectionString, string objectLifetime) : base(connectionString, objectLifetime)
    {
    }

    protected override void TouchActiveData()
    {
      var _cacheFieldInfo = typeof(SqlServerClientDataStore).BaseType.BaseType.GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic);

      if (_cacheFieldInfo != null)
      {
        ICache _cache = _cacheFieldInfo.GetValue(this) as ICache;
        if (_cache == null) return;

        foreach (var key in _cache.GetCacheKeys())
        {
          if (!string.IsNullOrEmpty(key) && _cache[key] != null)
          {
            this.TouchData(key);
            DataCount.DataClientDataWrites.Increment(1L);
          }
        }
      }
    }
  }
}