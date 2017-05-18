package com.linkedin.thirdeye.datasource.cache;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;

public class DashboardsCacheLoader extends CacheLoader<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardsCacheLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DashboardConfigManager dashboardConfigDAO;

  public DashboardsCacheLoader(DashboardConfigManager dashboardConfigDAO) {
    this.dashboardConfigDAO = dashboardConfigDAO;
  }

  @Override
  public String load(String collection) throws Exception {

    String jsonDashboards = null;
    try {
      LOGGER.info("Loading dashboards cache for {}", collection);
      List<String> dashboards = Utils.getDashboards(dashboardConfigDAO, collection);
      jsonDashboards = OBJECT_MAPPER.writeValueAsString(dashboards);
    } catch (Exception e) {
      LOGGER.error("Error while fetching dashboards for collection: " + collection, e);
    }
    return jsonDashboards;
  }
}

