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

package org.apache.pinot.thirdeye.datasource.csv;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.Grouping;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static org.apache.pinot.thirdeye.dataframe.Series.SeriesType.*;


/**
 * The type CSV third eye data source, which can make CSV file the data source of ThirdEye.
 * Can be used for testing purposes. The CSV file must have a column called 'timestamp', which is
 * the timestamp of the time series.
 */
public class CSVThirdEyeDataSource implements ThirdEyeDataSource {
  /**
   * The constant COL_TIMESTAMP. The name of the time stamp column.
   */
  public static final String COL_TIMESTAMP = "timestamp";

  public static final String DATA_SOURCE_NAME = CSVThirdEyeDataSource.class.getSimpleName();

  /**
   * The Data sets.
   */
  Map<String, DataFrame> dataSets;

  /**
   * The Translator from metric Id to metric name.
   */
  TranslateDelegator translator;

  /**
   * DataSource name
   */
  String name;

  /**
   * Factory method of CSVThirdEyeDataSource. Construct a CSVThirdEyeDataSource using data frames.
   *
   * @param dataSets the data sets
   * @param metricNameMap the metric name map
   * @return the CSVThirdEyeDataSource
   */
  public static CSVThirdEyeDataSource fromDataFrame(Map<String, DataFrame> dataSets, Map<Long, String> metricNameMap) {
    return new CSVThirdEyeDataSource(dataSets, metricNameMap);
  }

  /**
   * Factory method of CSVThirdEyeDataSource. Construct a CSVThirdEyeDataSource from data set URLs.
   *
   * @param dataSets the data sets in URL
   * @param metricNameMap the metric name map
   * @return the CSVThirdEyeDataSource
   * @throws Exception the exception
   */
  public static CSVThirdEyeDataSource fromUrl(Map<String, URL> dataSets, Map<Long, String> metricNameMap)
      throws Exception {
    Map<String, DataFrame> dataframes = new HashMap<>();
    for (Map.Entry<String, URL> source : dataSets.entrySet()) {
      try (InputStreamReader reader = new InputStreamReader(source.getValue().openStream())) {
        dataframes.put(source.getKey(), DataFrame.fromCsv(reader));
      }
    }

    return new CSVThirdEyeDataSource(dataframes, metricNameMap);
  }

  /**
   * This constructor is invoked by fromUrl
   *
   * @param dataSets the data sets
   * @param metricNameMap the static metric Id to metric name mapping.
   */
  CSVThirdEyeDataSource(Map<String, DataFrame> dataSets, Map<Long, String> metricNameMap) {
    this.dataSets = dataSets;
    this.translator = new StaticTranslator(metricNameMap);
    this.name = CSVThirdEyeDataSource.class.getSimpleName();
  }

  /**
   * This constructor is invoked by Java Reflection for initialize a ThirdEyeDataSource.
   *
   * @param properties the property to initialize this data source
   * @throws Exception the exception
   */
  public CSVThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    Map<String, DataFrame> dataframes = new HashMap<>();
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      try (InputStreamReader reader = new InputStreamReader(makeUrlFromPath(property.getValue().toString()).openStream())) {
        dataframes.put(property.getKey(), DataFrame.fromCsv(reader));
      }
    }

    this.dataSets = dataframes;
    this.translator = new DAOTranslator();
    this.name = MapUtils.getString(properties, "name", CSVThirdEyeDataSource.class.getSimpleName());
  }

  /**
   * Return the name of CSVThirdEyeDataSource.
   * @return the name of this CSVThirdEyeDataSource
   */
  @Override
  public String getName() {
    return this.name;
  }

  /**
   * Execute the request of querying CSV ThirdEye data source.
   * Supports filter operation using time stamp and dimensions.
   * Supports group by time stamp and dimensions.
   * Only supports SUM as the aggregation function for now.
   * @return a ThirdEyeResponse that contains the result of executing the request.
   */
  @Override
  public ThirdEyeResponse execute(final ThirdEyeRequest request) throws Exception {
    DataFrame df = new DataFrame();
    for (MetricFunction function : request.getMetricFunctions()) {
      final String inputName = translator.translate(function.getMetricId());
      final String outputName = function.toString();

      final MetricAggFunction aggFunction = function.getFunctionName();
      if (aggFunction != MetricAggFunction.SUM) {
        throw new IllegalArgumentException(String.format("Aggregation function '%s' not supported yet.", aggFunction));
      }

      DataFrame data = dataSets.get(function.getDataset());

      // filter constraints
      if (request.getStartTimeInclusive() != null) {
        data = data.filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return values[0] >= request.getStartTimeInclusive().getMillis();
          }
        }, COL_TIMESTAMP);
      }

      if (request.getEndTimeExclusive() != null) {
        data = data.filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return values[0] < request.getEndTimeExclusive().getMillis();
          }
        }, COL_TIMESTAMP);
      }

      if (request.getFilterSet() != null) {
        Multimap<String, String> filters = request.getFilterSet();
        for (final Map.Entry<String, Collection<String>> filter : filters.asMap().entrySet()) {
          data = data.filter(makeFilter(filter.getValue()), filter.getKey());
        }
      }

      data = data.dropNull(inputName);

      //
      // with grouping
      //
      if (request.getGroupBy() != null && request.getGroupBy().size() != 0) {
        Grouping.DataFrameGrouping dataFrameGrouping = data.groupByValue(request.getGroupBy());
        List<String> aggregationExps = new ArrayList<>();
        final String[] groupByColumns = request.getGroupBy().toArray(new String[0]);
        for (String groupByCol : groupByColumns) {
          aggregationExps.add(groupByCol + ":first");
        }
        aggregationExps.add(inputName + ":sum");

        if (request.getGroupByTimeGranularity() != null) {
          // group by both time granularity and column
          List<DataFrame.Tuple> tuples =
              dataFrameGrouping.aggregate(aggregationExps).getSeries().get("key").getObjects().toListTyped();
          for (final DataFrame.Tuple key : tuples) {
            DataFrame filteredData = data.filter(new Series.StringConditional() {
              @Override
              public boolean apply(String... values) {
                for (int i = 0; i < groupByColumns.length; i++) {
                  if (values[i] != key.getValues()[i]) {
                    return false;
                  }
                }
                return true;
              }
            }, groupByColumns);
            filteredData = filteredData.dropNull()
                .groupByInterval(COL_TIMESTAMP, request.getGroupByTimeGranularity().toMillis())
                .aggregate(aggregationExps);
            if (df.size() == 0) {
              df = filteredData;
            } else {
              df = df.append(filteredData);
            }
          }
          df.renameSeries(inputName, outputName);

        } else {
          // group by columns only
          df = dataFrameGrouping.aggregate(aggregationExps);
          df.dropSeries("key");
          df.renameSeries(inputName, outputName);
          df = df.sortedBy(outputName).reverse();

          if (request.getLimit() > 0) {
            df = df.head(request.getLimit());
          }
        }

        //
        // without dimension grouping
        //
      } else {
        if (request.getGroupByTimeGranularity() != null) {
          // group by time granularity only
          // TODO handle non-UTC time zone gracefully
          df = data.groupByInterval(COL_TIMESTAMP, request.getGroupByTimeGranularity().toMillis())
              .aggregate(inputName + ":sum");
          df.renameSeries(inputName, outputName);
        } else {
          // aggregation only
          df.addSeries(outputName, data.getDoubles(inputName).sum());
          df.addSeries(COL_TIMESTAMP, LongSeries.buildFrom(-1));
        }
      }

      df = df.dropNull(outputName);
    }

    // TODO handle non-dataset granularity gracefully
    TimeSpec timeSpec = new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT);
    if (request.getGroupByTimeGranularity() != null) {
      timeSpec = new TimeSpec("timestamp", request.getGroupByTimeGranularity(), TimeSpec.SINCE_EPOCH_FORMAT);
    }

    return new CSVThirdEyeResponse(request, timeSpec, df);
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return new ArrayList<>(dataSets.keySet());
  }

  @Override
  public void clear() throws Exception {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    if (!dataSets.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    return dataSets.get(dataset).getLongs(COL_TIMESTAMP).max().longValue();
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    if (!dataSets.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    Map<String, Series> data = dataSets.get(dataset).getSeries();
    Map<String, List<String>> output = new HashMap<>();
    for (Map.Entry<String, Series> entry : data.entrySet()) {
      if (entry.getValue().type() == STRING) {
        output.put(entry.getKey(), entry.getValue().unique().getStrings().toList());
      }
    }
    return output;
  }

  private interface TranslateDelegator {
    /**
     * translate a metric id to metric name
     *
     * @param metricId the metric id
     * @return the metric name as a string
     */
    String translate(Long metricId);
  }

  private static class DAOTranslator implements TranslateDelegator {
    /**
     * The translator that maps metric id to metric name based on a configDTO.
     */

    @Override
    public String translate(Long metricId) {
      MetricConfigDTO configDTO = DAORegistry.getInstance().getMetricConfigDAO().findById(metricId);
      if (configDTO == null) {
        throw new IllegalArgumentException(String.format("Can not find metric id %d", metricId));
      }
      return configDTO.getName();
    }
  }

  private static class StaticTranslator implements TranslateDelegator {

    /**
     * The Static translator that maps metric id to metric name based on a static map.
     */
    Map<Long, String> staticMap;

    /**
     * Instantiates a new Static translator.
     *
     * @param staticMap the static map
     */
    public StaticTranslator(Map<Long, String> staticMap) {
      this.staticMap = staticMap;
    }

    @Override
    public String translate(Long metricId) {
      return staticMap.get(metricId);
    }
  }

  private URL makeUrlFromPath(String input) {
    try {
      return new URL(input);
    } catch (MalformedURLException ignore) {
      // ignore
    }
    return this.getClass().getResource(input);
  }

  /**
   * Returns a filter function with inclusion and exclusion support
   *
   * @param values dimension filter values
   * @return StringConditional
   */
  private Series.StringConditional makeFilter(Collection<String> values) {
    final Set<String> exclusions = new HashSet<>(Collections2.filter(values, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String s) {
        return s != null && s.startsWith("!");
      }
    }));

    final Set<String> inclusions = new HashSet<>(values);
    inclusions.removeAll(exclusions);

    return new Series.StringConditional() {
      @Override
      public boolean apply(String... values) {
        return (inclusions.isEmpty() || inclusions.contains(values[0])) && !exclusions.contains("!" + values[0]);
      }
    };
  }
}


