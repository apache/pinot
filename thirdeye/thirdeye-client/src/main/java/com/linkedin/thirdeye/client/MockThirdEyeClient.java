package com.linkedin.thirdeye.client;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;

/**
 * Generates random data for a single collection according to provided StarTreeConfig.
 * @author jteoh
 */
public class MockThirdEyeClient extends BaseThirdEyeClient {
  public static final int DEFAULT_METRIC_VALUE_RANGE = 10000;
  public static final int DEFAULT_DIMENSION_VALUE_CARDINALITY = 5;
  private static final Random rand = new Random();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final StarTreeConfig config;
  private final int metricValueRange;
  private final int dimensionValueCardinality;

  public MockThirdEyeClient(StarTreeConfig config) {
    this(config, DEFAULT_METRIC_VALUE_RANGE, DEFAULT_DIMENSION_VALUE_CARDINALITY);
  }

  public MockThirdEyeClient(StarTreeConfig config, int metricValueRange,
      int dimensionValueCardinality) {
    this.config = config;
    this.metricValueRange = metricValueRange;
    this.dimensionValueCardinality = dimensionValueCardinality;
  }

  public static MockThirdEyeClient generateClientWithStarTreeProps(String collection,
      int numDimensions, int numMetrics, TimeGranularity dataBucket, int metricValueRange,
      int dimensionValueCardinality) {
    StarTreeConfig.Builder configBuilder = new StarTreeConfig.Builder().setCollection(collection);
    configBuilder.setDimensions(generateDimensions(generateRangeList("dim", numDimensions)));
    configBuilder.setMetrics(generateMetrics(generateRangeList("metric", numMetrics)));
    configBuilder.setTime(new TimeSpec("dummy", dataBucket, dataBucket, dataBucket));
    try {
      return new MockThirdEyeClient(configBuilder.build(), metricValueRange,
          dimensionValueCardinality);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static MockThirdEyeClient fromConfigFile(String starTreeConfigFilePath,
      int metricValueRange, int dimensionValueCardinality) {
    FileInputStream inp = null;
    try {
      inp = new FileInputStream(starTreeConfigFilePath);
      return new MockThirdEyeClient(StarTreeConfig.decode(inp), metricValueRange,
          dimensionValueCardinality);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    } finally {
      IOUtils.closeQuietly(inp);
    }
  }

  private static List<String> generateRangeList(String prefix, int count) {
    List<String> result = new LinkedList<>();
    for (int i = 1; i <= count; i++) {
      result.add(prefix + i);
    }
    return result;
  }

  // Defaults to int
  private static List<MetricSpec> generateMetrics(List<String> names) {
    List<MetricSpec> result = new LinkedList<>();
    for (String name : names) {
      result.add(new MetricSpec(name, MetricType.DOUBLE));
    }
    return result;
  }

  private static List<DimensionSpec> generateDimensions(List<String> names) {
    List<DimensionSpec> result = new LinkedList<>();
    for (String name : names) {
      result.add(new DimensionSpec(name));
    }
    return result;
  }

  @Override
  public ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception {
    ThirdEyeRawResponse response = new ThirdEyeRawResponse();
    StarTreeConfig config = getStarTreeConfig(request.getCollection());
    List<String> dimensions = config.getDimensionNames();

    response.setDimensions(dimensions);
    response.setMetrics(config.getMetricNames());

    Set<String> groupBy = request.getGroupBy();
    List<String> timestamps = getExpectedTimestamps(request);

    int metricCount = config.getMetrics().size();
    HashMap<String, Map<String, Number[]>> data = new HashMap<String, Map<String, Number[]>>();
    for (String dimensionKey : generateDimensionKeys(dimensions, groupBy)) {
      HashMap<String, Number[]> keyData = new HashMap<String, Number[]>();
      data.put(dimensionKey, keyData);
      for (String timestamp : timestamps) {
        keyData.put(timestamp, generateMetricData(metricCount));
      }
    }

    response.setData(data);
    return response;

  }

  private List<String> generateDimensionKeys(List<String> dimensions, Set<String> groupBy)
      throws Exception {
    if (Math.pow(dimensionValueCardinality, dimensions.size()) > 1000) {
      throw new IllegalArgumentException(
          "Mock client does not support more than 100 distinct dimension keys");
    }
    String baseKey = calculateBaseDimensionKey(dimensions, groupBy);

    List<List<String>> current = Collections.singletonList(Collections.<String> emptyList());
    for (int i = 0; i < dimensions.size(); i++) {
      List<List<String>> next = new LinkedList<>();
      String dimension = dimensions.get(i);
      if (groupBy.contains(dimension)) {
        continue;
      }
      for (int dimVal = 1; dimVal <= dimensionValueCardinality; dimVal++) {
        String dimensionValue = dimension + "_val" + dimVal;
        for (List<String> key : current) {
          LinkedList<String> newKey = new LinkedList<>(key);
          newKey.addFirst(dimensionValue);
          next.add(newKey);
        }
      }
      current = next;
    }

    List<String> results = new LinkedList<>();
    for (List<String> keyList : current) {
      String formattedKey = String.format(baseKey, keyList.toArray());
      String finalKey = MAPPER.writeValueAsString(formattedKey.split(","));
      results.add(finalKey);
    }
    return results;
  }

  private String calculateBaseDimensionKey(List<String> dimensions, Set<String> groupBy) {
    List<String> placeholders = new LinkedList<>();
    for (String dimension : dimensions) {
      if (groupBy.contains(dimension)) {
        placeholders.add("%s");
      } else {
        placeholders.add("*");
      }
    }
    return StringUtils.join(placeholders, ",");
  }

  private Number[] generateMetricData(int metricCount) {
    Number[] data = new Number[metricCount];
    for (int i = 0; i < metricCount; i++) {
      data[i] = (double) rand.nextInt(metricValueRange);
    }
    return data;
  }

  @Override
  public StarTreeConfig getStarTreeConfig(String collection) throws Exception {
    if (!config.getCollection().equals(collection)) {
      throw new IllegalArgumentException("Invalid collection: " + collection);
    }
    return config;
  }

  @Override
  public List<String> getCollections() throws Exception {
    return Collections.singletonList(config.getCollection());
  }

  /** Returns a segment descriptor from start of epoch until now. */
  @Override
  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    TimeSpec timeSpec = getStarTreeConfig(collection).getTime();
    long bucketMillis = timeSpec.getBucket().toMillis();
    long currentMillis = DateTime.now(DateTimeZone.UTC).getMillis();

    // round down to nearest unit
    currentMillis = currentMillis / bucketMillis * bucketMillis;

    SegmentDescriptor singletonDescriptor = new SegmentDescriptor(null, null, null,
        new DateTime(0, DateTimeZone.UTC), new DateTime(currentMillis, DateTimeZone.UTC));
    return Collections.singletonList(singletonDescriptor);
  }

  @Override
  public void clear() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

}
