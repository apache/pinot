package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.IngraphMetricConfigBean;

public class IngraphMetricConfigManagerImpl extends AbstractManagerImpl<IngraphMetricConfigDTO> implements IngraphMetricConfigManager {

  public IngraphMetricConfigManagerImpl() {
    super(IngraphMetricConfigDTO.class, IngraphMetricConfigBean.class);
  }
}
