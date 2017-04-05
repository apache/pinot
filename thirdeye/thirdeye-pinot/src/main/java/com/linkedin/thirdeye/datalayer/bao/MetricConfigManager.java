package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;


public interface MetricConfigManager extends AbstractManager<MetricConfigDTO> {

  List<MetricConfigDTO> findByDataset(String dataset);
  MetricConfigDTO findByMetricAndDataset(String metricName, String dataset);
  MetricConfigDTO findByAliasAndDataset(String alias, String dataset);
  List<MetricConfigDTO> findActiveByDataset(String dataset);
  List<MetricConfigDTO> findWhereNameLikeAndActive(String name);
  List<MetricConfigDTO> findByMetricName(String metricName);

}
