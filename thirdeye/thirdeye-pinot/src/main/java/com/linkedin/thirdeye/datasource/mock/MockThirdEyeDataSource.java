package com.linkedin.thirdeye.datasource.mock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.detection.ConfigUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTimeZone;


/**
 * MockThirdEyeDataSource generates time series based on generator configs. Once generated,
 * the data is cached in memory until the application terminates. This data source serves
 * testing and demo purposes.
 */
public class MockThirdEyeDataSource implements ThirdEyeDataSource {
  private final Map<String, MockDataset> datasets;

  /**
   * This constructor is invoked by Java Reflection to initialize a ThirdEyeDataSource.
   *
   * @param properties the properties to initialize this data source with
   * @throws Exception if properties cannot be parsed
   */
  public MockThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    this.datasets = new HashMap<>();

    Map<String, Object> config = ConfigUtils.getMap(properties.get("datasets"));
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      this.datasets.put(entry.getKey(), MockDataset.fromMap(
          ConfigUtils.<String, Object>getMap(entry.getValue())
      ));
    }
  }

  @Override
  public String getName() {
    return MockThirdEyeDataSource.class.getSimpleName();
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    // TODO implement execution
    // TODO implement data generation
    // TODO implement data caching
    throw new IllegalStateException("Not implemented yet");
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return new ArrayList<>(this.datasets.keySet());
  }

  @Override
  public void clear() throws Exception {
    // left blank
  }

  @Override
  public void close() throws Exception {
    // left blank
  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    if (!this.datasets.containsKey(dataset)) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", dataset));
    }

    return System.currentTimeMillis();
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    if (!this.datasets.containsKey(dataset)) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", dataset));
    }

    MockDataset mockDataset = this.datasets.get(dataset);

    SetMultimap<String, String> dimensions = extractDimensionValues(mockDataset.dimensions, mockDataset.generators, 0);

    Map<String, List<String>> output = new HashMap<>();
    for (Map.Entry<String, Collection<String>> entry : dimensions.asMap().entrySet()) {
      List<String> values = new ArrayList<>(entry.getValue());
      Collections.sort(values);
      output.put(entry.getKey(), values);
    }

    return output;
  }

  private SetMultimap<String, String> extractDimensionValues(List<String> dimensions, Map<String, Object> map, int level) {
    SetMultimap<String, String> output = HashMultimap.create();

    String key = dimensions.get(level);
    output.putAll(key, map.keySet());

    if (level + 1 >= dimensions.size()) {
      return output;
    }

    for (String value : map.keySet()) {
      output.putAll(extractDimensionValues(dimensions, (Map<String, Object>) map.get(value), level + 1));
    }

    return output;
  }

  /**
   * Container class for datasets and their generator configs
   */
  static final class MockDataset {
    final String name;
    final DateTimeZone timezone;
    final List<String> dimensions;
    final Map<String, Object> generators;

    MockDataset(String name, DateTimeZone timezone, List<String> dimensions, Map<String, Object> generators) {
      this.name = name;
      this.timezone = timezone;
      this.dimensions = dimensions;
      this.generators = generators;
    }

    static MockDataset fromMap(Map<String, Object> map) {
      return new MockDataset(
          MapUtils.getString(map, "name"),
          DateTimeZone.forID(MapUtils.getString(map, "timezone", "America/Los_Angeles")),
          ConfigUtils.<String>getList(map.get("dimensions")),
          ConfigUtils.<String, Object>getMap(map.get("generators")));
    }
  }
}
