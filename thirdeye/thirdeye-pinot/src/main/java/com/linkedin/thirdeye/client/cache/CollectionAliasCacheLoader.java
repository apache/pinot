package com.linkedin.thirdeye.client.cache;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;

/**
 * This cache holds the mapping between collection alias to collection name
 */
public class CollectionAliasCacheLoader extends CacheLoader<String, String> {

  private WebappConfigManager webappConfigDAO;

  public CollectionAliasCacheLoader(WebappConfigManager webappConfigDAO) {
    this.webappConfigDAO = webappConfigDAO;
  }

  @Override
  public String load(String collectionAlias) throws Exception {
    String collectionName = null;
    List<WebappConfigDTO> webappConfigs = webappConfigDAO.findByType(WebappConfigType.COLLECTION_CONFIG);
    for (WebappConfigDTO webappConfig : webappConfigs) {
      CollectionConfig collectionConfig = AbstractConfig.fromJSON(webappConfig.getConfig(), CollectionConfig.class);
      String alias = collectionConfig.getCollectionAlias();
      if (StringUtils.isNotEmpty(alias) && alias.equals(collectionAlias)) {
        collectionName = collectionConfig.getCollectionName();
      }
    }
    return collectionName;
  }

}
