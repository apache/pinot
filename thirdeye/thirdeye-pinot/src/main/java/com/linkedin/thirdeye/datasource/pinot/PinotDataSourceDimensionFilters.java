package com.linkedin.thirdeye.datasource.pinot;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;

/**
 * This class helps return dimension filters for a dataset from the Pinot data source
 */
public class PinotDataSourceDimensionFilters {
  private static final Logger LOG = LoggerFactory.getLogger(PinotDataSourceDimensionFilters.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  /**
   * This method gets the dimension filters for the given dataset from the pinot data source,
   * and returns them in json string format
   * @param dataset
   * @return dimension filters as json string
   */
  public String getDimensionFiltersJson(String dataset) {
    DateTime startDateTime = new DateTime(System.currentTimeMillis()).minusDays(7);
    DateTime endDateTime = new DateTime(System.currentTimeMillis());

    String jsonFilters = null;
    try {
      LOG.info("Loading dimension filters cache {}", dataset);
      List<String> dimensions = Utils.getSortedDimensionNames(dataset);
      Map<String, List<String>> filters =
          Utils.getFilters(CACHE_REGISTRY.getQueryCache(), dataset, "filters", dimensions, startDateTime, endDateTime);
      jsonFilters = OBJECT_MAPPER.writeValueAsString(filters);

    } catch (Exception e) {
      LOG.error("Error while fetching dimension values in filter drop down for collection: {}", dataset, e);
    }
    return jsonFilters;
  }

}
