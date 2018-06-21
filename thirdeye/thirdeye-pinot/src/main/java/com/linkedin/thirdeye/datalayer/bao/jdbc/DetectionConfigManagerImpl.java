package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DetectionConfigBean;


@Singleton
public class DetectionConfigManagerImpl extends AbstractManagerImpl<DetectionConfigDTO> implements DetectionConfigManager {
  public DetectionConfigManagerImpl() {
    super(DetectionConfigDTO.class, DetectionConfigBean.class);
  }


}
