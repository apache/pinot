package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class AlertConfigManagerImpl extends AbstractManagerImpl<AlertConfigDTO>
    implements AlertConfigManager {

  public AlertConfigManagerImpl() {
    super(AlertConfigDTO.class, AlertConfigBean.class);
  }

  @Override
  public List<AlertConfigDTO> findByActive(boolean active) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("active", active);
    return super.findByParams(filters);
  }
}
