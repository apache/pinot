package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DetectionAlertConfigBean;


@Singleton
public class DetectionAlertConfigManagerImpl extends AbstractManagerImpl<DetectionAlertConfigDTO> implements DetectionAlertConfigManager {
  public DetectionAlertConfigManagerImpl() {
    super(DetectionAlertConfigDTO.class, DetectionAlertConfigBean.class);
  }
}
