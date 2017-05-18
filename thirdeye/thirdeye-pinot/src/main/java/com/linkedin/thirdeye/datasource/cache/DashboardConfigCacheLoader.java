package com.linkedin.thirdeye.datasource.cache;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;

public class DashboardConfigCacheLoader extends CacheLoader<String, List<DashboardConfigDTO>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardConfigCacheLoader.class);
  private DashboardConfigManager dashboardConfigDAO;

  public DashboardConfigCacheLoader(DashboardConfigManager dashboardConfigDAO) {
    this.dashboardConfigDAO = dashboardConfigDAO;
  }

  @Override
  public List<DashboardConfigDTO> load(String collection) throws Exception {
    LOGGER.info("Loading DashboardConfigCache for {}", collection);
    List<DashboardConfigDTO> dashboardConfigs = dashboardConfigDAO.findByDataset(collection);
    return dashboardConfigs;
  }

}
