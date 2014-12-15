package com.linkedin.pinot.core.data.manager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;


/**
 * Provide static function to get PartitionDataManager implementation.
 * @author xiafu
 *
 */
public class ResourceDataManagerProvider {

  private static Map<String, Class<? extends ResourceDataManager>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends ResourceDataManager>>();

  static {
    keyToFunction.put("offline", OfflineResourceDataManager.class);
  }

  public static ResourceDataManager getResourceDataManager(ResourceDataManagerConfig resourceDataManagerConfig) {
    try {
      Class<? extends ResourceDataManager> cls =
          keyToFunction.get(resourceDataManagerConfig.getResourceDataManagerType().toLowerCase());
      if (cls != null) {
        ResourceDataManager resourceDataManager = (ResourceDataManager) cls.newInstance();
        resourceDataManager.init(resourceDataManagerConfig);
        return resourceDataManager;
      } else {
        throw new UnsupportedOperationException("No ResourceDataManager type found.");
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support ResourceDataManager type with - "
          + resourceDataManagerConfig.getResourceDataManagerType());
    }
  }

}
