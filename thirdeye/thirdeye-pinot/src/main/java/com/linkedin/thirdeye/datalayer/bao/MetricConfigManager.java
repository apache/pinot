package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Set;


public interface MetricConfigManager extends AbstractManager<MetricConfigDTO> {

  List<MetricConfigDTO> findByDataset(String dataset);
  MetricConfigDTO findByMetricAndDataset(String metricName, String dataset);
  MetricConfigDTO findByAliasAndDataset(String alias, String dataset);
  List<MetricConfigDTO> findActiveByDataset(String dataset);
  List<MetricConfigDTO> findWhereNameOrAliasLikeAndActive(String name);
  List<MetricConfigDTO> findWhereAliasLikeAndActive(Set<String> aliasParts);
  List<MetricConfigDTO> findByMetricName(String metricName);

}
