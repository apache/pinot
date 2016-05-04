package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.Builder;

/**
 * Parses query responses for {@link TimeSeriesRequest} objects.
 * This class currently does NOT support multiple group keys.
 * @author jteoh
 */
public class TimeSeriesResponseParser {
  private static final String OTHER = "OTHER"; // TODO this should be more generalized.

  // Roughly based off of TimeOnTimeResponseParser. The entry points are the parse* methods, which
  // in turn call the process* methods that loop through query results and use the extract* methods.
  public TimeSeriesResponseParser() {
  }

  /**
   * Intended for aggregation-only responses, coming from {@link TimeSeriesHandler}.
   * This method retrieves only the first value for each metric in <tt>response</tt> and uses that
   * data to generate a {@link TimeSeriesRow}. Behavior is undefined when multiple responses have
   * values for the same MetricFunction - don't use this for handling multiple time series requests!
   */
  public TimeSeriesRow parseAggregationOnlyResponse(TimeSeriesRequest request,
      Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap) {
    Map<MetricFunction, Double> metricData = processAggregateResponse(queryResultMap);

    Builder rowBuilder = new TimeSeriesRow.Builder();
    rowBuilder.setStart(request.getStart());
    rowBuilder.setEnd(request.getEnd());
    addMetricsToRow(metricData, rowBuilder);
    return rowBuilder.build();
  }

  /**
   * Extracts the first values for each metric in the {@link ThirdEyeResponse} objects for which the
   * request reference matches {@value TimeSeriesThirdEyeRequestGenerator#ALL}, merging all their
   * values into a single map. Behavior is undefined when multiple responses have values for the
   * same MetricFunction.
   */
  Map<MetricFunction, Double> processAggregateResponse(
      Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap) {
    Map<MetricFunction, Double> metricOnlyData = new HashMap<>();
    for (Entry<ThirdEyeRequest, ThirdEyeResponse> entry : queryResultMap.entrySet()) {
      ThirdEyeRequest thirdEyeRequest = entry.getKey();
      ThirdEyeResponse thirdEyeResponse = entry.getValue();
      if (CollectionUtils.isEmpty(thirdEyeRequest.getGroupBy())) {
        extractFirstMetricFunctionValues(metricOnlyData, thirdEyeResponse);
      }

    }
    return metricOnlyData;
  }

  /**
   * Retrieves the first value for each metric in the provided response and adds it to
   * <tt>aggregateData</tt>, returning the updated data in the process.
   */
  Map<MetricFunction, Double> extractFirstMetricFunctionValues(
      Map<MetricFunction, Double> aggregateData, ThirdEyeResponse thirdEyeResponse) {
    // similar to TimeOnTimeResponseParser.parseMetricOnlyResponse
    List<MetricFunction> metricFunctions = thirdEyeResponse.getMetricFunctions();
    for (MetricFunction metricFunction : metricFunctions) {
      if (thirdEyeResponse.getNumRowsFor(metricFunction) != 1) {
        // error?
      }
      Map<String, String> row = thirdEyeResponse.getRow(metricFunction, 0);
      // TODO figure out how to acquire the metric value. Currently expecting key to match
      // metricFunction.toString();
      String baselineValue = row.get(metricFunction.toString());
      aggregateData.put(metricFunction, Double.parseDouble(baselineValue));
    }
    return aggregateData;
  }

  private void addMetricsToRow(Map<MetricFunction, Double> metrics, Builder rowBuilder) {
    for (Entry<MetricFunction, Double> entry : metrics.entrySet()) {
      MetricFunction metricFunction = entry.getKey();
      Double value = entry.getValue();
      rowBuilder.addMetric(metricFunction, value);
    }
  }

  /**
   * Parses dimension groups into rows, with an additional row for "OTHER" category if totals do not
   * add up.
   */
  public List<TimeSeriesRow> parseGroupByDimensionResponse(TimeSeriesRequest timeSeriesRequest,
      Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap) {
    // based off of TimeOnTimeResponseParser.parseGroupByDimensionResponse
    Map<MetricFunction, Double> metricOnlyData = processAggregateResponse(queryResultMap);
    // Might not retrieve all dimension values, so compute the cumulative sum
    // of retrieved values and add "OTHER" group in the end.
    DoubleKeyMap<MetricFunction, String, Double> metricSumFromGroupByResponse =
        new DoubleKeyMap<>();
    // Map<MetricFunction,Map<dimensionName,Map<dimensionValue,Map<(baseline|current), Double>>>>
    Map<MetricFunction, Map<String, Map<String, Double>>> metricGroupByDimensionData =
        processGroupByDimensionResponse(queryResultMap);
    // we need a build for each dimensionName, dimensionValue combination to properly add the
    // metrics
    RowBuilderCache rowBuilderCache = new RowBuilderCache();
    for (Entry<MetricFunction, Map<String, Map<String, Double>>> metricEntry : metricGroupByDimensionData
        .entrySet()) {
      MetricFunction metricFunction = metricEntry.getKey();
      Map<String, Map<String, Double>> dimensionDataMap = metricEntry.getValue();
      for (String dimensionName : dimensionDataMap.keySet()) {
        Map<String, Double> map = dimensionDataMap.get(dimensionName);

        double dimensionSum = 0.0;
        for (String dimensionValue : map.keySet()) {
          Double value = map.get(dimensionValue);
          TimeSeriesRow.Builder rowBuilder = rowBuilderCache.get(dimensionName, dimensionValue);
          rowBuilder.addMetric(metricFunction, value);
        }
        metricSumFromGroupByResponse.put(metricFunction, dimensionName, dimensionSum);
      }
    }

    computeOtherRows(rowBuilderCache, metricOnlyData, metricSumFromGroupByResponse);
    // TODO this should actually be aligned to the data start and end rather than request.
    return rowBuilderCache.buildAllRows(timeSeriesRequest.getStart(), timeSeriesRequest.getEnd());
  }

  /** Computes the OTHER category and adds it into the appropriate rows. */
  private RowBuilderCache computeOtherRows(RowBuilderCache rowBuilderCache,
      Map<MetricFunction, Double> metricOnlyData,
      DoubleKeyMap<MetricFunction, String, Double> metricSumFromGroupByResponse) {
    for (Entry<MetricFunction, Map<String, Double>> metricEntry : metricSumFromGroupByResponse
        .toMap().entrySet()) {
      MetricFunction metricFunction = metricEntry.getKey();
      double total = metricOnlyData.get(metricFunction);
      for (Entry<String, Double> dimensionEntry : metricEntry.getValue().entrySet()) {
        String dimensionName = dimensionEntry.getKey();
        double dimensionSum = dimensionEntry.getValue();
        double difference = total - dimensionSum;
        // only add OTHER group if the sum is different from the total.
        if (difference > 0) {
          Builder builder = rowBuilderCache.get(dimensionName, OTHER);
          builder.addMetric(metricFunction, difference);
        }
      }
    }
    return rowBuilderCache;

  }

  /**
   * Extracts all dimension values for the first grouped dimension according to each
   * request-response pairing. Return type is of form:
   * MetricFunction->Dimension->DimensionValue->MetricValue
   * This return format may change once multi-dimension grouping is supported.
   * Behavior is undefined when multiple responses contain the same MetricFunction.
   * This method ignores any ThirdEyeResponses for which the request reference is
   * {@value TimeSeriesThirdEyeRequestGenerator#ALL}
   */
  Map<MetricFunction, Map<String, Map<String, Double>>> processGroupByDimensionResponse(
      Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap) {
    TripleKeyMap<MetricFunction, String, String, Double> metricGroupByDimensionData =
        new TripleKeyMap<>(false, true, true);

    for (Entry<ThirdEyeRequest, ThirdEyeResponse> entry : queryResultMap.entrySet()) {
      ThirdEyeRequest thirdEyeRequest = entry.getKey();
      ThirdEyeResponse thirdEyeResponse = entry.getValue();
      if (CollectionUtils.isNotEmpty(thirdEyeRequest.getGroupBy())) {
        extractMetricGroupByDimensionResponse(metricGroupByDimensionData, thirdEyeRequest,
            thirdEyeResponse);
      }
    }
    return metricGroupByDimensionData.toMap();
  }

  /**
   * Extracts all dimension values for the first grouped dimension. Return type is of form:
   * MetricFunction->Dimension->DimensionValue->MetricValue
   * This return format may change once multi-dimension grouping is supported.
   */
  TripleKeyMap<MetricFunction, String, String, Double> extractMetricGroupByDimensionResponse(
      TripleKeyMap<MetricFunction, String, String, Double> dimensionData,
      ThirdEyeRequest thirdEyeRequest, ThirdEyeResponse thirdEyeResponse) {
    // similar to TimeSeriesResponseParser.parseMetricGroupByDimensionResponse
    List<String> groupBy = thirdEyeRequest.getGroupBy();
    if (groupBy == null || groupBy.size() != 1) {
      // TODO throw error?
    }
    // TODO we only group by at most 1 dimension - ideally this should be a loop on the grouped
    // dimensions.
    String dimensionName = thirdEyeRequest.getGroupBy().get(0);

    List<MetricFunction> metricFunctions = thirdEyeResponse.getMetricFunctions();
    for (MetricFunction metricFunction : metricFunctions) {
      // one row for each group by
      int rows = thirdEyeResponse.getNumRowsFor(metricFunction);
      for (int i = 0; i < rows; i++) {
        Map<String, String> row = thirdEyeResponse.getRow(metricFunction, i);
        String dimensionValue = row.get(dimensionName);
        Double metricValue = Double.parseDouble(row.get(metricFunction.toString()));
        dimensionData.put(metricFunction, dimensionName, dimensionValue, metricValue);
      }
    }
    return dimensionData;
  }

  // Util class to automatically create and reuse TimeSeriesRow.Builder instances for
  private class RowBuilderCache {
    private final DoubleKeyMap<String, String, Builder> rowMap = new DoubleKeyMap<>(false, true);

    public Builder get(String dimensionName, String dimensionValue) {
      if (!rowMap.containsKey(dimensionName, dimensionValue)) {
        Builder builder = new TimeSeriesRow.Builder();
        builder.setDimensionName(dimensionName);
        builder.setDimensionValue(dimensionValue);
        rowMap.put(dimensionName, dimensionValue, builder);
      }
      return rowMap.get(dimensionName, dimensionValue);
    }

    public List<TimeSeriesRow> buildAllRows(DateTime start, DateTime end) {
      List<TimeSeriesRow> rows = new ArrayList<>();
      for (Builder builder : rowMap.values()) {
        builder.setStart(start);
        builder.setEnd(end);
        rows.add(builder.build());
      }
      return rows;
    }
  }

  /**
   * Abstracted 2-level pseudo-map. This is used as opposed to multikey map so that intermediate
   * results can still be retrieved and different key types can be used. <br/>
   * This class intentionally does NOT support operations that allow access to the Map<K2, V>
   * instances to discourage incorrect usage of this class. If that is required, one should call the
   * toMap function. <br/>
   * This class supports key ordering via the boolean-based constructor.
   */
  class DoubleKeyMap<K1, K2, V> {
    private final Map<K1, Map<K2, V>> map;
    private final boolean orderedKey2;

    /** Returns a pseudo-map with no restriction on ordering. */
    public DoubleKeyMap() {
      this(false, false);
    }

    /** Returns a pseudo-map with key ordering as provided. */
    public DoubleKeyMap(boolean orderedKey1, boolean orderedKey2) {
      if (orderedKey1) {
        map = new TreeMap<K1, Map<K2, V>>();
      } else {
        map = new HashMap<K1, Map<K2, V>>();
      }
      this.orderedKey2 = orderedKey2;
    }

    public V get(K1 firstKey, K2 secondKey) {
      if (!map.containsKey(firstKey)) {
        return null;
      }
      return map.get(firstKey).get(secondKey);
    }

    public V put(K1 firstKey, K2 secondKey, V value) {
      if (!map.containsKey(firstKey)) {
        Map<K2, V> subMap;
        if (orderedKey2) {
          subMap = new TreeMap<K2, V>();
        } else {
          subMap = new HashMap<K2, V>();
        }
        map.put(firstKey, subMap);
      }
      Map<K2, V> subMap = map.get(firstKey);
      return subMap.put(secondKey, value);
    }

    public boolean containsKey(K1 firstKey, K2 secondKey) {
      return get(firstKey, secondKey) != null;
    }

    public Collection<V> values() {
      List<V> values = new ArrayList<>();
      for (Map<K2, V> submap : map.values()) {
        values.addAll(submap.values());
      }
      return values;
    }

    public Map<K1, Map<K2, V>> toMap() {
      return map;
    }
  }

  /**
   * Abstracted 3-level hashmap. This is used as opposed to multikey map so that intermediate
   * results can still be retrieved and different key types can be used. <br/>
   * This class intentionally does NOT support operations that allow access to the Map<K2, V>
   * instances to discourage incorrect usage of this class. If that is required, one should call
   * the toMap function. <br/>
   * This class supports key ordering via the boolean-based constructor.
   */
  class TripleKeyMap<K1, K2, K3, V> {
    // This is really just a map wrapper around the previous NestedMap.
    private final Map<K1, DoubleKeyMap<K2, K3, V>> map;
    private final boolean orderedKey1;
    private final boolean orderedKey2;
    private final boolean orderedKey3;

    /** Returns a pseudo-map with no restriction on ordering. */
    public TripleKeyMap() {
      this(false, false, false);
    }

    /** Returns a pseudo-map with key ordering as provided. */
    public TripleKeyMap(boolean orderedKey1, boolean orderedKey2, boolean orderedKey3) {
      if (orderedKey1) {
        map = new TreeMap<K1, DoubleKeyMap<K2, K3, V>>();
      } else {
        map = new HashMap<K1, DoubleKeyMap<K2, K3, V>>();
      }
      this.orderedKey1 = orderedKey1;
      this.orderedKey2 = orderedKey2;
      this.orderedKey3 = orderedKey3;
    }

    public V get(K1 firstKey, K2 secondKey, K3 thirdKey) {
      if (!map.containsKey(firstKey)) {
        return null;
      }
      return map.get(firstKey).get(secondKey, thirdKey);
    }

    public V put(K1 firstKey, K2 secondKey, K3 thirdKey, V value) {
      if (!map.containsKey(firstKey)) {
        map.put(firstKey, new DoubleKeyMap<K2, K3, V>(orderedKey2, orderedKey3));
      }
      DoubleKeyMap<K2, K3, V> nestedMap = map.get(firstKey);
      return nestedMap.put(secondKey, thirdKey, value);
    }

    public boolean containsKey(K1 firstKey, K2 secondKey, K3 thirdKey) {
      return get(firstKey, secondKey, thirdKey) != null;
    }

    public Collection<V> values() {
      List<V> values = new ArrayList<>();
      for (DoubleKeyMap<K2, K3, V> submap : map.values()) {
        values.addAll(submap.values());
      }
      return values;
    }

    public Map<K1, Map<K2, Map<K3, V>>> toMap() {
      Map<K1, Map<K2, Map<K3, V>>> result;
      if (orderedKey1) {
        result = new TreeMap<K1, Map<K2, Map<K3, V>>>();
      } else {
        result = new HashMap<K1, Map<K2, Map<K3, V>>>();
      }
      for (Entry<K1, DoubleKeyMap<K2, K3, V>> entry : map.entrySet()) {
        K1 key = entry.getKey();
        DoubleKeyMap<K2, K3, V> nestedMap = entry.getValue();
        result.put(key, nestedMap.toMap());
      }
      return result;
    }
  }
}
