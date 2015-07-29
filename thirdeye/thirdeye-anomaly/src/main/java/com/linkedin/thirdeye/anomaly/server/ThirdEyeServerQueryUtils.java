package com.linkedin.thirdeye.anomaly.server;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.util.SqlUtils;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

public class ThirdEyeServerQueryUtils {

  private ThirdEyeServerQueryUtils() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeServerQueryUtils.class);

  private static final Joiner COMMA = Joiner.on(",");

  /**
   * Use a LRU cache based on url to result
   */
  private static boolean enableCache = true;
  private static ThirdEyeQueryCache<String, StarTreeConfig> starTreeCache = new ThirdEyeQueryCache<>(20);
  private static ThirdEyeQueryCache<String, String> queryDataCache = new ThirdEyeQueryCache<>(100);

  /**
   * @param driverConfig
   * @return
   *  The starTreeConfig corresponding to the collection
   */
  public static synchronized StarTreeConfig getStarTreeConfig(AnomalyDetectionDriverConfig driverConfig)
      throws IOException {
    String url;
    url = "http://" + driverConfig.getHost() + ":" + driverConfig.getPort() + "/collections/" + driverConfig.getName();
    LOGGER.info("getting star-tree : {}", url);

    // use the cache
    if (enableCache) {
      StarTreeConfig cached = starTreeCache.get(url);
      if (cached != null) {
        LOGGER.info("cache hit for {}", url);
        return cached;
      }
    }

    Response response = Request.Get(url).execute();
    HttpResponse httpResponse = response.returnResponse();
    if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      throw new IOException();
    }
    StarTreeConfig result =  StarTreeConfig.decode(new ByteArrayInputStream(EntityUtils.toByteArray(
        httpResponse.getEntity())));

    // cache the result
    if (enableCache) {
      starTreeCache.put(url, result);
    }

    return result;
  }

  /**
   * @param collection
   * @param dimensions
   * @param metrics
   * @param aggregationFunction
   * @param timeRange
   * @return
   *  The anomaly detection dataset with at least the data in the requested timeRange
   */
  public static synchronized ThirdEyeServerQueryResult runQuery(
      AnomalyDetectionDriverConfig collection,
      Map<String, String> dimensionValues,
      List<MetricSpec> metrics,
      TimeGranularity aggregationGranularity,
      TimeRange timeRange) throws IOException {
    String metricFunction = buildMetricFunction(aggregationGranularity, metrics);

    DateTime start = new DateTime(timeRange.getStart(), DateTimeZone.UTC);
    // make the start time more generic
    if (enableCache) {
      start = start.withMillisOfDay(0);
    }
    DateTime end = new DateTime(timeRange.getEnd(), DateTimeZone.UTC);

    String sql = SqlUtils.getSql(metricFunction, collection.getName(), start, end, dimensionValues);

    String url;
    try {
      url = "http://" + collection.getHost() + ":" + collection.getPort() + "/query/"
          + URLEncoder.encode(sql,"UTF-8");
      LOGGER.info("getting data between {} and {} : {}", start, end, url);
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("failed to generate ThirdEye server query", e);
      return null;
    }

    // use the cache
    if (enableCache) {
      String cached = queryDataCache.get(url);
      if (cached != null) {
        LOGGER.info("cache hit for {}", url);
        return new ThirdEyeServerQueryResult(metrics, cached);
      }
    }

    Response response = Request.Get(url).execute();
    HttpResponse httpResponse = response.returnResponse();
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (statusCode != HttpStatus.SC_OK) {
      throw new IOException("Status code not 200 - status=" + statusCode + " url=" + url);
    }

    String responseString = EntityUtils.toString(httpResponse.getEntity());
    ThirdEyeServerQueryResult result = new ThirdEyeServerQueryResult(metrics, responseString);

    // cache the result
    if (enableCache) {
      queryDataCache.put(url, responseString);
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
