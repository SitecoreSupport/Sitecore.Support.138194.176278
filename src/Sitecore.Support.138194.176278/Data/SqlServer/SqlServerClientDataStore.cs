using Sitecore.Caching;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics.PerformanceCounters;
using System;
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

    protected override void CompactData()
    {
      var batchSize = Configuration.Settings.LinkDatabase.MaximumBatchSize;
      DateTime time = DateTime.UtcNow - ObjectLifetime;

      string sql = "DELETE TOP ({2}batchSize{3}) FROM {0}ClientData{1} WHERE {0}Accessed{1} < {2}maxDate{3} SELECT @@ROWCOUNT";
      object[] parameters = new object[] { "batchSize", batchSize, "maxDate", time };

      var affectedRows = 0;

      do
      {
        affectedRows = this._api.Execute(sql, parameters);
      }
      while (affectedRows > 0);
    }

  }
}