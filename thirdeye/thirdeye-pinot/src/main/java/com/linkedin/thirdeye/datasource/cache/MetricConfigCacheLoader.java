package com.linkedin.thirdeye.datasource.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

public class MetricConfigCacheLoader extends CacheLoader<MetricDataset, MetricConfigDTO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricConfigCacheLoader.class);
  private MetricConfigManager metricConfigDAO;

  public MetricConfigCacheLoader(MetricConfigManager metricConfigDAO) {
    this.metricConfigDAO = metricConfigDAO;
  }


  @Override
  public MetricConfigDTO load(MetricDataset metricDataset) {
    LOGGER.debug("Loading MetricConfigCache for metric {} of {}", metricDataset.getMetricName(),
        metricDataset.getDataset());
    MetricConfigDTO metricConfig = metricConfigDAO.findByMetricAndDataset(metricDataset.getMetricName(),
        metricDataset.getDataset());
    return metricConfig;
  }

}
