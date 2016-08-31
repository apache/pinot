package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.db.entity.WebappConfig;

public class WebappConfigDAO  extends AbstractJpaDAO<WebappConfig> {

  public WebappConfigDAO() {
    super(WebappConfig.class);
  }

  @Transactional
  public List<WebappConfig> findByCollection(String collection) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    return super.findByParams(filterParams);
  }

  @Transactional
  public List<WebappConfig> findByType(WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

  @Transactional
  public List<WebappConfig> findByCollectionAndType(String collection, WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

}
