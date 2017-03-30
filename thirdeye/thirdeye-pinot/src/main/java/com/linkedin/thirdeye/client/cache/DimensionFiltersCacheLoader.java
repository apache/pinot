package com.linkedin.thirdeye.client.cache;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.dashboard.Utils;

public class DimensionFiltersCacheLoader extends CacheLoader<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionFiltersCacheLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private QueryCache queryCache;

  public DimensionFiltersCacheLoader(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  @Override
  public String load(String dataset) throws Exception {
    DateTime startDateTime = new DateTime(System.currentTimeMillis()).minusDays(7);
    DateTime endDateTime = new DateTime(System.currentTimeMillis());

    String jsonFilters = null;
    try {
      LOGGER.info("Loading dimension filters cache {}", dataset);
      List<String> dimensions = Utils.getSortedDimensionNames(dataset);
      Map<String, List<String>> filters =
          Utils.getFilters(queryCache, dataset, "filters", dimensions, startDateTime, endDateTime);
      jsonFilters = OBJECT_MAPPER.writeValueAsString(filters);

    } catch (Exception e) {
      LOGGER.error("Error while fetching dimension values in filter drop down for collection: {}",
          dataset, e);
    }

    return jsonFilters;
  }
}

