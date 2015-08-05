package com.linkedin.thirdeye.anomaly.server;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;

public class ThirdEyeServerQueryUtils {

  private ThirdEyeServerQueryUtils() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeServerQueryUtils.class);

  private static final Joiner COMMA = Joiner.on(",");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Use a LRU cache based on url to result
   */
  private static boolean enableCache = true;
  private static ThirdEyeQueryCache<String, StarTreeConfig> starTreeCache = new ThirdEyeQueryCache<>(20);

  // It would be more efficient to cache parsed ThirdEyeQueryResult objects, but MetricTimeSeries is not thread safe.
  private static ThirdEyeQueryCache<String, QueryResult> queryDataCache = new ThirdEyeQueryCache<>(100);

  /**
   * @param driverConfig
   * @return
   *  The starTreeConfig corresponding to the collection
   */
  public static synchronized StarTreeConfig getStarTreeConfig(AnomalyDetectionDriverConfig driverConfig)
      throws IOException {
    String urlString = "http://" + driverConfig.getThirdEyeServerHost() + ":" + driverConfig.getThirdEyeServerPort() + "/collections/"
      + driverConfig.getCollectionName();
    LOGGER.info("getting star-tree : {}", urlString);

    // use the cache
    if (enableCache) {
      StarTreeConfig cached = starTreeCache.get(urlString);
      if (cached != null) {
        LOGGER.info("cache hit for {}", urlString);
        return cached;
      }
    }

    URL url = new URL(urlString);
    StarTreeConfig result = OBJECT_MAPPER.readValue(new InputStreamReader(url.openStream(), "UTF-8"),
        StarTreeConfig.class);

    // cache the result
    if (enableCache) {
      starTreeCache.put(urlString, result);
    }

    return result;
  }

  /**
   * @param collection
   * @param dimensions
   * @param metricSpecs
   * @param aggregationFunction
   * @param timeRange
   * @return
   *  The anomaly detection dataset with at least the data in the requested timeRange
   */
  public static synchronized ThirdEyeServerQueryResult runQuery(
      AnomalyDetectionDriverConfig collection,
      Map<String, String> dimensionValues,
      List<MetricSpec> metricSpecs,
      TimeGranularity aggregationGranularity,
      TimeRange timeRange) throws IOException {
    String metricFunction = buildMetricFunction(aggregationGranularity, metricSpecs);

    DateTime start = new DateTime(timeRange.getStart(), DateTimeZone.UTC);
    // make the start time more generic
    if (enableCache) {
      start = start.withMillisOfDay(0);
    }
    DateTime end = new DateTime(timeRange.getEnd(), DateTimeZone.UTC);

    String sql = SqlUtils.getSql(metricFunction, collection.getCollectionName(), start, end, dimensionValues);

    String urlString = "http://" + collection.getThirdEyeServerHost() + ":" + collection.getThirdEyeServerPort()
        + "/query/" + URLEncoder.encode(sql,"UTF-8");
    LOGGER.info("getting data between {} and {} : {}", start, end, urlString);

    // use the cache
    if (enableCache) {
      QueryResult cachedQueryResult = queryDataCache.get(urlString);
      if (cachedQueryResult != null) {
        LOGGER.info("cache hit for {}", urlString);
        return new ThirdEyeServerQueryResult(metricSpecs, cachedQueryResult);
      }
    }

    URL url = new URL(urlString);
    QueryResult queryResult = OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")),
        QueryResult.class);

    ThirdEyeServerQueryResult result = new ThirdEyeServerQueryResult(metricSpecs, queryResult);

    // cache the result
    if (enableCache) {
      queryDataCache.put(urlString, queryResult);
    }

    return result;
  }

  private static String buildMetricFunction(TimeGranularity aggregationGranularity, List<MetricSpec> metrics) {
    List<String> metricNames = new ArrayList<>(metrics.size());
    for (MetricSpec metric : metrics) {
      metricNames.add(metric.getName());
    }
    return String.format("AGGREGATE_%d_%s(%s)", aggregationGranularity.getSize(),
          aggregationGranularity.getUnit().toString().toUpperCase(), COMMA.join(metricNames));
  }

  public static boolean isEnableCache() {
    return enableCache;
  }

  public static void setEnableCache(boolean enableCache) {
    ThirdEyeServerQueryUtils.enableCache = enableCache;
  };

}
