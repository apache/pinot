package org.apache.pinot.segment.local.function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;


public class GroovySecurityConfigManager {
  private static LoadingCache<Integer, GroovyStaticAnalyzerConfig> _configCache;
  private static HelixManager _helixManager;

  public GroovySecurityConfigManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _configCache = CacheBuilder.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<>() {
          @Override
          @Nonnull
          public GroovyStaticAnalyzerConfig load(@Nonnull Integer key)
              throws Exception {
            Stat stat = new Stat();
            ZNRecord record = _helixManager.getHelixPropertyStore().get(
                "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer",
                stat, AccessOption.PERSISTENT);
            return GroovyStaticAnalyzerConfig.fromZNRecord(record);
          }
        });
  }

  public void setConfig(GroovyStaticAnalyzerConfig config) throws Exception {
    ZNRecord zr = config.toZNRecord();
    _helixManager.getHelixPropertyStore().set(
        "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer",
        zr,
        AccessOption.PERSISTENT
    );
  }

  public GroovyStaticAnalyzerConfig getConfig() throws Exception {
    Stat stat = new Stat();
    ZNRecord record = _helixManager.getHelixPropertyStore().get(
        "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer",
        stat, AccessOption.PERSISTENT);
    return GroovyStaticAnalyzerConfig.fromZNRecord(record);
  }
}
