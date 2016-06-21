package com.linkedin.thirdeye.client.cache;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

/**
 * This cache holds the mapping between collection alias to collection name
 */
public class CollectionAliasCacheLoader extends CacheLoader<String, String> {

  private AbstractConfigDAO<CollectionConfig> collectionConfigDAO;

  public CollectionAliasCacheLoader(AbstractConfigDAO<CollectionConfig> collectionConfigDAO) {
    this.collectionConfigDAO = collectionConfigDAO;
  }

  @Override
  public String load(String collectionAlias) throws Exception {
    String collectionName = null;
    List<CollectionConfig> collectionConfigs = collectionConfigDAO.findAll();
    for (CollectionConfig collectionConfig : collectionConfigs) {
      String alias = collectionConfig.getCollectionAlias();
      if (StringUtils.isNotEmpty(alias) && alias.equals(collectionAlias)) {
        collectionName = collectionConfig.getCollectionName();
      }
    }
    return collectionName;
  }

}
