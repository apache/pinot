package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;


public interface IngraphMetricConfigManager extends AbstractManager<IngraphMetricConfigDTO> {

  List<IngraphMetricConfigDTO> findByDashboard(String dashboardName);
  IngraphMetricConfigDTO findByDashboardAndMetricName(String dashboardName, String metricName);
  IngraphMetricConfigDTO findByRrdName(String rrdName);


}
