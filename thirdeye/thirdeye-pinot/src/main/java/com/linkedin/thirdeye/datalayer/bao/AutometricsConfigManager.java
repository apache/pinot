package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.AutometricsConfigDTO;


public interface AutometricsConfigManager extends AbstractManager<AutometricsConfigDTO> {

  List<AutometricsConfigDTO> findByDashboard(String dashboard);
  AutometricsConfigDTO findByDashboardAndMetric(String dashboard, String metric);
  AutometricsConfigDTO findByRrd(String rrd);


}
