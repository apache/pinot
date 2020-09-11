/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.sql.SqlDataset;
import org.apache.pinot.thirdeye.datasource.sql.SqlUtils;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.pinot.thirdeye.dataframe.DataFrame.COL_TIME;
import static org.apache.pinot.thirdeye.dataframe.DataFrame.COL_VALUE;
import static org.apache.pinot.thirdeye.datasource.sql.SqlResponseCacheLoader.*;


/**
 * MockThirdEyeDataSource generates time series based on generator configs. Once generated,
 * the data is cached in memory until the application terminates. This data source serves
 * testing and demo purposes.
 */
public class MockThirdEyeDataSource implements ThirdEyeDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(MockThirdEyeDataSource.class);

  private static double COMPONENT_ALPHA_DAILY = 0.25;
  private static double COMPONENT_ALPHA_WEEKLY = 0.5;

  private static final String PROP_POPULATE_META_DATA = "populateMetaData";
  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_DATASET_METRICS = "metrics";
  private static final DateTime MIN_DATETIME = DateTime.parse("1970-01-01");

  final Map<String, MockDataset> datasets;

  final Map<String, DataFrame> datasetData;
  final Map<Long, String> metricNameMap;

  final String name;

  final CSVThirdEyeDataSource delegate;

  /**
   * This constructor is invoked by Java Reflection to initialize a ThirdEyeDataSource.
   *
   * @param properties the properties to initialize this data source with
   * @throws Exception if properties cannot be parsed
   */
  public MockThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    this.name = MapUtils.getString(properties, "name", MockThirdEyeDataSource.class.getSimpleName());

    // datasets
    this.datasets = new HashMap<>();
    Map<String, Object> config = ConfigUtils.getMap(properties.get(DATASETS));
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      this.datasets.put(entry.getKey(), MockDataset.fromMap(
          entry.getKey(), ConfigUtils.<String, Object>getMap(entry.getValue())
      ));
    }

    LOG.info("Found {} datasets: {}", this.datasets.size(), this.datasets.keySet());

    // mock data
    final long lookback = MapUtils.getLongValue(properties, PROP_LOOKBACK, 28);
    final long tEnd = System.currentTimeMillis();
    final long tStart = tEnd - TimeUnit.DAYS.toMillis(lookback);

    LOG.info("Generating data for time range {} to {}", tStart, tEnd);

    // mock data per sub-dimension
    Map<Tuple, DataFrame> rawData = new HashMap<>();
    for (MockDataset dataset : this.datasets.values()) {
      for (String metric : dataset.metrics.keySet()) {
        String[] basePrefix = new String[] { dataset.name, PROP_DATASET_METRICS, metric };

        Collection<Tuple> paths = makeTuples(dataset.metrics.get(metric), basePrefix, dataset.dimensions.size() + basePrefix.length);
        for (Tuple path : paths) {
          LOG.info("Generating '{}'", Arrays.asList(path.values));

          Map<String, Object> metricConfig = resolveTuple(config, path);
          rawData.put(path, makeData(metricConfig,
              new DateTime(tStart, dataset.timezone),
              new DateTime(tEnd, dataset.timezone),
              dataset.granularity));
        }
      }
    }

    // merge data
    long metricNameCounter = 0;
    this.datasetData = new HashMap<>();
    this.metricNameMap = new HashMap<>();

    // per dataset
    List<String> sortedDatasets = new ArrayList<>(this.datasets.keySet());
    Collections.sort(sortedDatasets);

    for (String datasetName : sortedDatasets) {
      MockDataset dataset = this.datasets.get(datasetName);
      Map<String, DataFrame> metricData = new HashMap<>();

      List<String> indexes = new ArrayList<>();
      indexes.add(COL_TIME);
      indexes.addAll(dataset.dimensions);

      // per metric
      List<String> sortedMetrics = new ArrayList<>(dataset.metrics.keySet());
      Collections.sort(sortedMetrics);

      for (String metric : sortedMetrics) {
        this.metricNameMap.put(1 + metricNameCounter++, metric);

        String[] prefix = new String[] { dataset.name, PROP_DATASET_METRICS, metric };
        Collection<Tuple> tuples = filterTuples(rawData.keySet(), prefix);

        // per dimension
        List<DataFrame> dimensionData = new ArrayList<>();
        for (Tuple tuple : tuples) {
          String metricName = tuple.values[2]; // ["dataset", "metrics", "metric", ...]

          DataFrame dfExpanded = new DataFrame(rawData.get(tuple)).renameSeries(COL_VALUE, metricName);

          for (int i = 0; i < dataset.dimensions.size(); i++) {
            String dimValue = tuple.values[i + 3];
            String dimName = dataset.dimensions.get(i);
            dfExpanded.addSeries(dimName, StringSeries.fillValues(dfExpanded.size(), dimValue));
          }

          dfExpanded.setIndex(indexes);

          dimensionData.add(dfExpanded);
        }

        metricData.put(metric, DataFrame.concatenate(dimensionData));
      }

      List<String> fields = new ArrayList<>();
      fields.add(COL_TIME + ":LONG");
      for (String name : dataset.dimensions) {
        fields.add(name + ":STRING");
      }
      for (String name : dataset.metrics.keySet()) {
        fields.add(name + ":DOUBLE");
      }

      DataFrame dfDataset = DataFrame.builder(fields).build().setIndex(indexes);
      for (Map.Entry<String, DataFrame> entry : metricData.entrySet()) {
        String metricName = entry.getKey();
        dfDataset = dfDataset.joinOuter(entry.getValue())
            .renameSeries(metricName + DataFrame.COLUMN_JOIN_RIGHT, metricName)
            .dropSeries(metricName + DataFrame.COLUMN_JOIN_LEFT);
      }

      this.datasetData.put(dataset.name, dfDataset);

      LOG.info("Merged '{}' with {} rows and {} columns", dataset.name, dfDataset.size(), dfDataset.getSeriesNames().size());
    }

    this.delegate = CSVThirdEyeDataSource.fromDataFrame(this.datasetData, this.metricNameMap);

    // auto onboarding support
    if (MapUtils.getBooleanValue(properties, PROP_POPULATE_META_DATA, false)) {
      MetadataSourceConfig metadataSourceConfig = new MetadataSourceConfig();
      metadataSourceConfig.setProperties(properties);

      AutoOnboardMockDataSource onboarding = new AutoOnboardMockDataSource(metadataSourceConfig);

      onboarding.runAdhoc();
    }

    loadMockCSVData(properties);
  }

  private void loadMockCSVData(Map<String, Object> properties) throws Exception {
    if (properties.containsKey(H2)) {
      DataSource h2DataSource = new DataSource();
      Map<String, Object> objMap = ConfigUtils.getMap(properties.get(H2));

      h2DataSource.setInitialSize(INIT_CONNECTIONS);
      h2DataSource.setMaxActive(MAX_CONNECTIONS);
      String h2User = (String) objMap.get(USER);
      String h2Password = getPassword(objMap);
      String h2Url = (String) objMap.get(DB);
      h2DataSource.setUsername(h2User);
      h2DataSource.setPassword(h2Password);
      h2DataSource.setUrl(h2Url);

      // Timeout before an abandoned(in use) connection can be removed.
      h2DataSource.setRemoveAbandonedTimeout(ABANDONED_TIMEOUT);
      h2DataSource.setRemoveAbandoned(true);

      DateTime maxDateTime = MIN_DATETIME;
      List<String[]> h2Rows = new ArrayList<>();
      if (objMap.containsKey(DATASETS)) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          List<Object> objs = (List) objMap.get(DATASETS);
          for (Object obj : objs) {
            SqlDataset dataset = mapper.convertValue(obj, SqlDataset.class);

            String[] tableNameSplit = dataset.getTableName().split("\\.");
            String tableName = tableNameSplit[tableNameSplit.length - 1];

            List<String> metrics = new ArrayList<>(dataset.getMetrics().keySet());

            SqlUtils.createTableOverride(h2DataSource, tableName, dataset.getTimeColumn(), metrics, dataset.getDimensions());
            SqlUtils.onBoardSqlDataset(dataset);

            DateTimeFormatter fmt = DateTimeFormat.forPattern(dataset.getTimeFormat()).withZone(DateTimeZone.forID(dataset.getTimezone()));

            if (dataset.getDataFile().length() > 0) {
              String thirdEyeConfigDir = System.getProperty("dw.rootDir");
              String fileURI = thirdEyeConfigDir + "/data/" + dataset.getDataFile();
              File file = new File(fileURI);
              try (Scanner scanner = new Scanner(file)) {
                String columnNames = scanner.nextLine();
                while (scanner.hasNextLine()) {
                  String line = scanner.nextLine();
                  String[] columnValues = line.split(",");
                  DateTime dateTime = DateTime.parse(columnValues[0], fmt);
                  if (dateTime.isAfter(maxDateTime)) {
                    maxDateTime = dateTime;
                  }
                  h2Rows.add(columnValues);
                }
                int days = (int) ((DateTime.now().getMillis() - maxDateTime.getMillis()) / TimeUnit.DAYS.toMillis(1));
                for (String[] columnValues : h2Rows) {
                  columnValues[0] = fmt.print(DateTime.parse(columnValues[0], fmt).plusDays(days));
                  SqlUtils.insertCSVRow(h2DataSource, tableName, columnNames, columnValues);
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage());
          throw e;
        }
      }
    }
  }

  private String getPassword(Map<String, Object> objMap) {
    String password = (String) objMap.get(PASSWORD);
    password = (password == null) ? "" : password;
    return password;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    return this.delegate.execute(request);
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
    return this.delegate.getMaxDataTime(dataset);
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return this.delegate.getDimensionFilters(dataset);
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
    double daily = MapUtils.getDoubleValue(config, "daily", mean);
    double weekly = MapUtils.getDoubleValue(config, "weekly", daily);
    NormalDistribution dist = new NormalDistribution(mean, std);

    DateTime origin = start.withFields(DataFrameUtils.makeOrigin(PeriodType.days()));
    while (origin.isBefore(end)) {
      if (origin.isBefore(start)) {
        origin = origin.plus(interval);
        continue;
      }

      timestamps.add(origin.getMillis());

      double compDaily = weekly * (COMPONENT_ALPHA_WEEKLY + Math.sin(origin.getDayOfWeek() / 7.0 * 2 * Math.PI + 1) / 2 * (1 - COMPONENT_ALPHA_WEEKLY));
      double compHourly = daily * (COMPONENT_ALPHA_DAILY + Math.sin(origin.getHourOfDay() / 24.0 * 2 * Math.PI + 1) / 2 * (1 - COMPONENT_ALPHA_DAILY));
      double compEpsilon = dist.sample();

      values.add((double) Math.max(Math.round(compDaily + compHourly + compEpsilon), 0));
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

  /**
   * Helper class for depth-first iteration of metric dimensions
   */
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

  /**
   * Helper class for comparable tuples
   */
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
