package com.linkedin.thirdeye.anomaly.util;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 *
 */
public class ThirdEyeRequestUtils {

  private static final Joiner COMMA = Joiner.on(",");

  public static final String NULL_GROUP_BY = null;

  /**
   * @param collection
   * @param dimensions
   * @param metricNames
   * @param aggregationFunction
   * @param timeRange
   * @return
   *  The anomaly detection dataset with at least the data in the requested timeRange
   * @throws Exception
   */
  public static ThirdEyeRequest buildRequest(
      String collection,
      String groupByDimension,
      Map<String, String> fixedDimensionValues,
      List<String> metricNames,
      TimeGranularity aggregationGranularity,
      TimeRange timeRange) throws Exception
  {
    DateTime start = new DateTime(timeRange.getStart(), DateTimeZone.UTC);
    // make the start time more generic
    start = start.withMillisOfDay(0);
    DateTime end = new DateTime(timeRange.getEnd(), DateTimeZone.UTC);

    ThirdEyeRequest request = new ThirdEyeRequest()
      .setCollection(collection)
      .setStartTime(start)
      .setEndTime(end)
      .setMetricFunction(buildMetricFunction(aggregationGranularity, metricNames));

    if (groupByDimension != NULL_GROUP_BY) {
      request.setGroupBy(groupByDimension);
    }

    for (Entry<String, String> entry : fixedDimensionValues.entrySet()) {
      request.addDimensionValue(entry.getKey(), entry.getValue());
    }

    return request;
  }

  private static String buildMetricFunction(TimeGranularity aggregationGranularity, List<String> metricNames) {
    return String.format("AGGREGATE_%d_%s(%s)", aggregationGranularity.getSize(),
        aggregationGranularity.getUnit().toString().toUpperCase(), COMMA.join(metricNames));
  }
}
