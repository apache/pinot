package com.linkedin.pinot.server.partition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.server.conf.PartitionDataManagerConfig;


/**
 * Provide static function to get PartitionDataManager implementation.
 * @author xiafu
 *
 */
public class PartitionProvider {

  private static Map<String, Class<? extends PartitionDataManager>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends PartitionDataManager>>();

  static {
    keyToFunction.put("offline", OfflinePartitionDataManager.class);
  }

  public static PartitionDataManager getPartitionDataManager(PartitionDataManagerConfig partitionConfig) {
    try {
      Class<? extends PartitionDataManager> cls = keyToFunction.get(partitionConfig.getPartitionType().toLowerCase());
      if (cls != null) {
        PartitionDataManager partitionDataManager = (PartitionDataManager) cls.newInstance();
        partitionDataManager.init(partitionConfig);
        return partitionDataManager;
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support partition type with - " + partitionConfig.getPartitionType());
    }
    throw new UnsupportedOperationException("No partition type with - " + partitionConfig.getPartitionType());
  }
}
