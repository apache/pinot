package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.persist.Transactional;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;

public class WebappConfigManager  extends AbstractManager<WebappConfigDTO> {

  public WebappConfigManager() {
    super(WebappConfigDTO.class);
  }

  @Transactional
  public List<WebappConfigDTO> findByCollection(String collection) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    return super.findByParams(filterParams);
  }

  @Transactional
  public List<WebappConfigDTO> findByType(WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

  @Transactional
  public List<WebappConfigDTO> findByCollectionAndType(String collection, WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

}
