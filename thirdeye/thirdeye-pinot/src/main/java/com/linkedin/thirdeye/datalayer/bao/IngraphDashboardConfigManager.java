package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;


public interface IngraphDashboardConfigManager extends AbstractManager<IngraphDashboardConfigDTO> {

  IngraphDashboardConfigDTO findByName(String dashboardName);


}
