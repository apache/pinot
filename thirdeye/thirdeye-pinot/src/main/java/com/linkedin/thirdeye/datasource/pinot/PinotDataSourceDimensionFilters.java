package com.linkedin.thirdeye.datasource.pinot;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;

/**
 * This class helps return dimension filters for a dataset from the Pinot data source
 */
public class PinotDataSourceDimensionFilters {
  private static final Logger LOG = LoggerFactory.getLogger(PinotDataSourceDimensionFilters.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  /**
   * This method gets the dimension filters for the given dataset from the pinot data source,
   * and returns them as map of dimension name to values
   * @param dataset
   * @return dimension filters map
   */
  public Map<String, List<String>> getDimensionFilters(String dataset) {
    DateTime startDateTime = new DateTime(System.currentTimeMillis()).minusDays(7);
    DateTime endDateTime = new DateTime(System.currentTimeMillis());

    Map<String, List<String>> filters = null;
    try {
      LOG.info("Loading dimension filters cache {}", dataset);
      List<String> dimensions = Utils.getSortedDimensionNames(dataset);
      filters = Utils.getFilters(CACHE_REGISTRY.getQueryCache(), dataset, "filters", dimensions, startDateTime, endDateTime);
    } catch (Exception e) {
      LOG.error("Error while fetching dimension values in filter drop down for collection: {}", dataset, e);
    }
    return filters;
  }

}
