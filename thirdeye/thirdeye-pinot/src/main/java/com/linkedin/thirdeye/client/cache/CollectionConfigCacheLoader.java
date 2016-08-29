package com.linkedin.thirdeye.client.cache;

import java.util.List;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.db.dao.WebappConfigDAO;
import com.linkedin.thirdeye.db.entity.WebappConfig;

public class CollectionConfigCacheLoader extends CacheLoader<String, CollectionConfig> {

  private WebappConfigDAO webappConfigDAO;

  public CollectionConfigCacheLoader(WebappConfigDAO webappConfigDAO) {
    this.webappConfigDAO = webappConfigDAO;
  }

  @Override
  public CollectionConfig load(String collection) throws Exception {
    CollectionConfig collectionConfig = null;
    List<WebappConfig> webappConfigs = webappConfigDAO
        .findByCollectionAndType(collection, WebappConfigType.COLLECTION_CONFIG);
    if (!webappConfigs.isEmpty()) {
      collectionConfig = AbstractConfig.fromJSON(webappConfigs.get(0).getConfig(), CollectionConfig.class);
    }
    return collectionConfig;
  }

}
