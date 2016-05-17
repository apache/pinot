package com.linkedin.thirdeye.client.cache;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

public class CollectionConfigCacheLoader extends CacheLoader<String, CollectionConfig> {

  private AbstractConfigDAO<CollectionConfig> collectionConfigDAO;

  public CollectionConfigCacheLoader(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionConfig> collectionConfigDAO) {
    this.collectionConfigDAO = collectionConfigDAO;
  }

  @Override
  public CollectionConfig load(String collection) throws Exception {
    CollectionConfig collectionConfig = collectionConfigDAO.findById(collection);
    return collectionConfig;
  }

}
