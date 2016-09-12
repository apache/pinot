package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.WebappConfigBean;

public class WebappConfigManagerImpl extends AbstractManagerImpl<WebappConfigDTO>
    implements WebappConfigManager {

  public WebappConfigManagerImpl() {
    super(WebappConfigDTO.class, WebappConfigBean.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.linkedin.thirdeye.datalayer.bao.IWebappConfigManager#findByCollection(java.lang.String)
   */
  @Override
  public List<WebappConfigDTO> findByCollection(String collection) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    return super.findByParams(filterParams);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.linkedin.thirdeye.datalayer.bao.IWebappConfigManager#findByType(com.linkedin.thirdeye.
   * dashboard.configs.WebappConfigFactory.WebappConfigType)
   */
  @Override
  public List<WebappConfigDTO> findByType(WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

  @Override
  public List<WebappConfigDTO> findByCollectionAndType(String collection, WebappConfigType type) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("type", type);
    return super.findByParams(filterParams);
  }

}
