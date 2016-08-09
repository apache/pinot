package com.linkedin.thirdeye.db.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory.WebappConfigType;
import com.linkedin.thirdeye.db.entity.WebappConfig;

public class WebappConfigDAO  extends AbstractJpaDAO<WebappConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(WebappConfigDAO.class);

  // find by collection
  private static final String FIND_BY_COLLECTION = "SELECT wc FROM WebappConfig wc "
      + "WHERE wc.collection = :collection ";

  //find by config type
   private static final String FIND_BY_CONFIG_TYPE = "SELECT wc FROM WebappConfig wc "
       + "WHERE wc.configType = :configType ";

  // find by collection and config type
  private static final String FIND_BY_COLLECTION_AND_CONFIG_TYPE = "SELECT wc FROM WebappConfig wc "
      + "WHERE wc.collection = :collection "
      + "AND wc.configType = :configType";

  public WebappConfigDAO() {
    super(WebappConfig.class);
  }

  public List<WebappConfig> findByCollection(String collection) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION, WebappConfig.class)
        .setParameter("collection", collection)
        .getResultList();
  }

  public List<WebappConfig> findByConfigType(WebappConfigType configType) {
    return getEntityManager().createQuery(FIND_BY_CONFIG_TYPE, WebappConfig.class)
        .setParameter("configType", configType)
        .getResultList();
  }

  public List<WebappConfig> findByCollectionAndConfigType(String collection, WebappConfigType configType) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_AND_CONFIG_TYPE, WebappConfig.class)
        .setParameter("collection", collection)
        .setParameter("configType", configType)
        .getResultList();
  }

}
