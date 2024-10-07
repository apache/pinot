package org.apache.pinot.segment.local.function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;


public class GroovySecurityConfigManager {
  private static LoadingCache<Integer, Long> _configCache;
  private static HelixManager _helixManager;

  public GroovySecurityConfigManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _configCache = CacheBuilder.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<Integer, Long>() {
          @Override
          public Long load(Integer key)
              throws Exception {
            Stat stat = new Stat();
            ZNRecord record = _helixManager.getHelixPropertyStore().get("/", stat, 0);
            return record.getModifiedTime();
          }
        });
  }
}
