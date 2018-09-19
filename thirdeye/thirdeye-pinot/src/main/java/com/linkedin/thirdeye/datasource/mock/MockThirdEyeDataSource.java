package com.linkedin.thirdeye.datasource.mock;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import com.linkedin.thirdeye.detection.ConfigUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * MockThirdEyeDataSource generates time series based on generator configs. Once generated,
 * the data is cached in memory until the application terminates. This data source serves
 * testing and demo purposes.
 */
public class MockThirdEyeDataSource implements ThirdEyeDataSource {
  final Map<String, MockDataset> datasets;

  final Map<String, Map<String, List<String>>> dimensionFiltersCache;

  final Map<Tuple, DataFrame> dimensionData;

  final Map<String, DataFrame> datasetData;
  final Map<Long, String> metricNameMap;

  final CSVThirdEyeDataSource delegate;

  /**
   * This constructor is invoked by Java Reflection to initialize a ThirdEyeDataSource.
   *
   * @param properties the properties to initialize this data source with
   * @throws Exception if properties cannot be parsed
   */
  public MockThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    // datasets
    this.datasets = new HashMap<>();
    Map<String, Object> config = ConfigUtils.getMap(properties.get("datasets"));
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      this.datasets.put(entry.getKey(), MockDataset.fromMap(
          entry.getKey(), ConfigUtils.<String, Object>getMap(entry.getValue())
      ));
    }

    // dimension filters
    this.dimensionFiltersCache = new HashMap<>();
    for (MockDataset dataset : this.datasets.values()) {
      this.dimensionFiltersCache.put(dataset.name, extractDimensionFilters(dataset));
    }

    // mock data
    final long tEnd = System.currentTimeMillis();
    final long tStart = tEnd - TimeUnit.DAYS.toMillis(28);

    this.dimensionData = new HashMap<>();
    for (MockDataset dataset : this.datasets.values()) {
      for (String metric : dataset.metrics.keySet()) {
        String[] basePrefix = new String[] { dataset.name, "metrics", metric };

        Collection<Tuple> paths = makeTuples(dataset.metrics.get(metric), basePrefix, dataset.dimensions.size() + basePrefix.length);
        for (Tuple path : paths) {
          Map<String, Object> metricConfig = resolveTuple(config, path);
          this.dimensionData.put(path, makeData(metricConfig,
              new DateTime(tStart, dataset.timezone),
              new DateTime(tEnd, dataset.timezone),
              dataset.granularity));
        }
      }
    }
    
    long metricNameCounter = 0;
    this.datasetData = new HashMap<>();
    this.metricNameMap = new HashMap<>();
    for (MockDataset dataset : this.datasets.values()) {
      String[] prefix = new String[] { dataset.name };
      Collection<Tuple> tuples = filterTuples(this.dimensionData.keySet(), prefix);
      List<DataFrame> dataFrames = new ArrayList<>();

      for (Tuple tuple : tuples) {
        this.metricNameMap.put(metricNameCounter++, tuple.values[2]);
        dataFrames.add(this.dimensionData.get(tuple));
      }

      this.datasetData.put(dataset.name, DataFrame.concatenate(dataFrames));
    }

    this.delegate = CSVThirdEyeDataSource.fromDataFrame(this.datasetData, this.metricNameMap);
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
    if (!this.dimensionFiltersCache.containsKey(dataset)) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", dataset));
    }
    return this.dimensionFiltersCache.get(dataset);
  }

  /**
   * Returns a full filter multimap for a given MockDataset.
   *
   * @param dataset mock dataset config
   * @return filter multimap
   */
  private static Map<String, List<String>> extractDimensionFilters(MockDataset dataset) {
    SetMultimap<String, String> allDimensions = HashMultimap.create();

    for (Map<String, Object> metric : dataset.metrics.values()) {
      allDimensions.putAll(extractDimensionValues(dataset.dimensions, metric, 0));
    }

    Map<String, List<String>> output = new HashMap<>();
    for (Map.Entry<String, Collection<String>> entry : allDimensions.asMap().entrySet()) {
      List<String> values = new ArrayList<>(entry.getValue());
      Collections.sort(values);
      output.put(entry.getKey(), values);
    }

    return output;
  }

  /**
   * Recursively extracts dimension names and values from a metric config
   *
   * @param dimensions  dimension levels to explore
   * @param map nested config with metric generators
   * @param level current recursion depth
   * @return multimap with dimension filters
   */
  private static SetMultimap<String, String> extractDimensionValues(List<String> dimensions, Map<String, Object> map, int level) {
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
   * Returns a DataFrame populated with mock data for a given config and time range.
   *
   * @param config metric generator config
   * @param start start time
   * @param end end time
   * @param interval time granularity
   * @return DataFrame with mock data
   */
  private static DataFrame makeData(Map<String, Object> config, DateTime start, DateTime end, Period interval) {
    List<Long> timestamps = new ArrayList<>();
    List<Double> values = new ArrayList<>();

    double mean = MapUtils.getDoubleValue(config, "mean", 0);
    double std = MapUtils.getDoubleValue(config, "std", 1);
    NormalDistribution dist = new NormalDistribution(mean, std);

    DateTime origin = start.withFields(DataFrameUtils.makeOrigin(PeriodType.days()));
    while (origin.isBefore(end)) {
      if (origin.isBefore(start)) {
        origin = origin.plus(interval);
        continue;
      }

      timestamps.add(origin.getMillis());
      values.add(Math.max(dist.sample(), 0));
      origin = origin.plus(interval);
    }

    return new DataFrame()
        .addSeries(COL_TIME, ArrayUtils.toPrimitive(timestamps.toArray(new Long[0])))
        .addSeries(COL_VALUE, ArrayUtils.toPrimitive(values.toArray(new Double[0])))
        .setIndex(COL_TIME);
  }

  /**
   * Returns list of tuples for (a metric's) nested generator configs.
   *
   * @param map nested config with generator configs
   * @param maxDepth max expected level of depth
   * @return metric tuples
   */
  private static List<Tuple> makeTuples(Map<String, Object> map, String[] basePrefix, int maxDepth) {
    List<Tuple> tuples = new ArrayList<>();

    LinkedList<MetricTuple> stack = new LinkedList<>();
    stack.push(new MetricTuple(basePrefix, map));

    while (!stack.isEmpty()) {
      MetricTuple tuple = stack.pop();
      if (tuple.prefix.length >= maxDepth) {
        tuples.add(new Tuple(tuple.prefix));

      } else {
        for (Map.Entry<String, Object> entry : tuple.map.entrySet()) {
          Map<String, Object> nested = (Map<String, Object>) entry.getValue();
          String[] prefix = Arrays.copyOf(tuple.prefix, tuple.prefix.length + 1);
          prefix[prefix.length - 1] = entry.getKey();

          stack.push(new MetricTuple(prefix, nested));
        }
      }
    }

    return tuples;
  }

  /**
   * Returns the bottom-level config for a given metric tuple from the root of a nested generator config
   *
   * @param map nested config with generator configs
   * @param path metric generator path
   * @return generator config
   */
  private static Map<String, Object> resolveTuple(Map<String, Object> map, Tuple path) {
    for (String element : path.values) {
      map = (Map<String, Object>) map.get(element);
    }
    return map;
  }

  /**
   * Returns a filtered collection of tuples for a given prefix
   *
   * @param tuples collections of tuples
   * @param prefix reuquired prefix
   * @return filtered collection of tuples
   */
  private static Collection<Tuple> filterTuples(Collection<Tuple> tuples, final String[] prefix) {
    return Collections2.filter(tuples, new Predicate<Tuple>() {
      @Override
      public boolean apply(@Nullable Tuple tuple) {
        if (tuple == null || tuple.values.length < prefix.length) {
          return false;
        }

        for (int i = 0; i < prefix.length; i++) {
          if (!StringUtils.equals(tuple.values[i], prefix[i])) {
            return false;
          }
        }

        return true;
      }
    });
  }

  /**
   * Container class for datasets and their generator configs
   */
  static final class MockDataset {
    final String name;
    final DateTimeZone timezone;
    final List<String> dimensions;
    final Map<String, Map<String, Object>> metrics;
    final Period granularity;

    MockDataset(String name, DateTimeZone timezone, List<String> dimensions, Map<String, Map<String, Object>> metrics, Period granularity) {
      this.name = name;
      this.timezone = timezone;
      this.dimensions = dimensions;
      this.metrics = metrics;
      this.granularity = granularity;
    }

    static MockDataset fromMap(String name, Map<String, Object> map) {
      return new MockDataset(
          name,
          DateTimeZone.forID(MapUtils.getString(map, "timezone", "America/Los_Angeles")),
          ConfigUtils.<String>getList(map.get("dimensions")),
          ConfigUtils.<String, Map<String, Object>>getMap(map.get("metrics")),
          ConfigUtils.parsePeriod(MapUtils.getString(map, "granularity", "1hour")));
    }
  }

  static final class MetricTuple {
    final String[] prefix;
    final Map<String, Object> map;

    MetricTuple(String[] prefix, Map<String, Object> map) {
      this.prefix = prefix;
      this.map = map;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MetricTuple that = (MetricTuple) o;
      return Arrays.equals(prefix, that.prefix) && Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(map);
      result = 31 * result + Arrays.hashCode(prefix);
      return result;
    }
  }

  static final class Tuple {
    final String[] values;

    public Tuple(String[] values) {
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tuple tuple = (Tuple) o;
      return Arrays.equals(values, tuple.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
