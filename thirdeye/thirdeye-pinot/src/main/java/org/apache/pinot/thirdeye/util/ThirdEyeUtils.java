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

package org.apache.pinot.thirdeye.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.Validation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.thirdeye.anomaly.views.AnomalyTimelinesView;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean.COMPARE_MODE;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeDataUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalQuery;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.GrouperWrapperConstants.PROP_DETECTOR_COMPONENT_NAME;
import static org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO.TIME_SERIES_SNAPSHOT_KEY;

public abstract class ThirdEyeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeUtils.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();
  private static final String TWO_DECIMALS_FORMAT = "#,###.##";
  private static final String MAX_DECIMALS_FORMAT = "#,###.#####";
  private static final String DECIMALS_FORMAT_TOKEN = "#";
  private static final String PROP_DETECTOR_COMPONENT_NAME_DELIMETER = ",";

  private static final int DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE = 50;
  private static final int DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 100;
  private static final int DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 8192;

  // How much data to prefetch to warm up the cache
  private static final long DEFAULT_CACHING_PERIOD_LOOKBACK = -1;
  private static final long CACHING_PERIOD_LOOKBACK_DAILY = TimeUnit.DAYS.toMillis(90);
  private static final long CACHING_PERIOD_LOOKBACK_HOURLY = TimeUnit.DAYS.toMillis(60);
  // disable minute level cache warm up
  private static final long CACHING_PERIOD_LOOKBACK_MINUTELY = -1;

  public static final long DETECTION_TASK_MAX_LOOKBACK_WINDOW = TimeUnit.DAYS.toMillis(7);

  private ThirdEyeUtils () {

  }

  /**
   * Returns or modifies a filter that can be for querying the results corresponding to the given dimension map.
   *
   * For example, if a dimension map = {country=IN,page_name=front_page}, then the two entries will be added or
   * over-written to the given filter.
   *
   * Note that if the given filter contains an entry: country=["IN", "US", "TW",...], then this entry is replaced by
   * country=IN.
   *
   * @param dimensionMap the dimension map to add to the filter
   * @param filterToDecorate if it is null, a new filter will be created; otherwise, it is modified.
   * @return a filter that is modified according to the given dimension map.
   */
  public static Multimap<String, String> getFilterSetFromDimensionMap(DimensionMap dimensionMap,
      Multimap<String, String> filterToDecorate) {
    if (filterToDecorate == null) {
      filterToDecorate = HashMultimap.create();
    }

    for (Map.Entry<String, String> entry : dimensionMap.entrySet()) {
      String dimensionName = entry.getKey();
      String dimensionValue = entry.getValue();
      // If dimension value is "OTHER", then we need to get all data and calculate "OTHER" part.
      // In order to reproduce the data for "OTHER", the filter should remain as is.
      if ( !dimensionValue.equalsIgnoreCase("OTHER") ) {
        // Only add the specific dimension value to the filter because other dimension values will not be used
        filterToDecorate.removeAll(dimensionName);
        filterToDecorate.put(dimensionName, dimensionValue);
      }
    }

    return filterToDecorate;
  }

  public static String convertMultiMapToJson(Multimap<String, String> multimap)
      throws JsonProcessingException {
    Map<String, Collection<String>> map = multimap.asMap();
    return OBJECT_MAPPER.writeValueAsString(map);
  }

  public static Multimap<String, String> convertToMultiMap(String json) {
    ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
    if (json == null) {
      return multimap;
    }
    try {
      TypeReference<Map<String, ArrayList<String>>> valueTypeRef =
          new TypeReference<Map<String, ArrayList<String>>>() {
          };
      Map<String, ArrayList<String>> map;

      map = OBJECT_MAPPER.readValue(json, valueTypeRef);
      for (Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
        ArrayList<String> valueList = entry.getValue();
        ArrayList<String> trimmedList = new ArrayList<>();
        for (String value : valueList) {
          trimmedList.add(value.trim());
        }
        multimap.putAll(entry.getKey(), trimmedList);
      }
      return multimap;
    } catch (IOException e) {
      LOG.error("Error parsing json:{} message:{}", json, e.getMessage());
    }
    return multimap;
  }

  public static String getSortedFiltersFromJson(String filterJson) {
    Multimap<String, String> filterMultiMap = convertToMultiMap(filterJson);
    String sortedFilters = ThirdEyeDataUtils.getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }
    return sortedFilters;
  }

  private static String getTimeFormatString(DatasetConfigDTO datasetConfig) {
    String timeFormat = datasetConfig.getTimeFormat();
    if (timeFormat.startsWith(DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      timeFormat = getSDFPatternFromTimeFormat(timeFormat);
    }
    return timeFormat;
  }

  /**
   * Returns the time spec of the buckets (data points) in the specified dataset config. For additive dataset, this
   * method returns the same time spec as getTimestampTimeSpecFromDatasetConfig; however, for non-additive dataset,
   * this method return the time spec for buckets (data points) instead of the one for the timestamp in the backend
   * database. For example, the data points of a non-additive dataset could be 5-MINUTES granularity, but timestamp's
   * granularity could be 1-Milliseconds. For additive dataset, the discrepancy is not an issue, but it could be
   * a problem for non-additive dataset.
   *
   * @param datasetConfig the given dataset config
   *
   * @return the time spec of the buckets (data points) in the specified dataset config.
   */
  public static TimeSpec getTimeSpecFromDatasetConfig(DatasetConfigDTO datasetConfig) {
    String timeFormat = getTimeFormatString(datasetConfig);
    TimeSpec timespec = new TimeSpec(datasetConfig.getTimeColumn(),
        new TimeGranularity(datasetConfig.bucketTimeGranularity()), timeFormat);
    return timespec;
  }

  /**
   * Returns the time spec of the timestamp in the specified dataset config. The timestamp time spec is mainly used
   * for constructing the queries to backend database. For most use case, this method returns the same time spec as
   * getTimeSpecFromDatasetConfig(); however, if the dataset is non-additive, then getTimeSpecFromDatasetConfig
   * should be used unless the application is related to database queries.
   *
   * @param datasetConfig the given dataset config
   *
   * @return the time spec of the timestamp in the specified dataset config.
   */
  public static TimeSpec getTimestampTimeSpecFromDatasetConfig(DatasetConfigDTO datasetConfig) {
    String timeFormat = getTimeFormatString(datasetConfig);
    TimeSpec timespec = new TimeSpec(datasetConfig.getTimeColumn(),
        new TimeGranularity(datasetConfig.getTimeDuration(), datasetConfig.getTimeUnit()), timeFormat);
    return timespec;
  }

  private static String getSDFPatternFromTimeFormat(String timeFormat) {
    String pattern = timeFormat;
    String[] tokens = timeFormat.split(":", 2);
    if (tokens.length == 2) {
      pattern = tokens[1];
    }
    return pattern;
  }

  public static MetricExpression getMetricExpressionFromMetricConfig(MetricConfigDTO metricConfig) {
    String expression = null;
    if (metricConfig.isDerived()) {
      expression = metricConfig.getDerivedMetricExpression();
    } else {
      expression = MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId();
    }
    MetricExpression metricExpression = new MetricExpression(metricConfig.getName(), expression,
        metricConfig.getDefaultAggFunction(), metricConfig.getDataset());
    return metricExpression;
  }

  // TODO: Write parser instead of looking for occurrence of every metric
  public static String substituteMetricIdsForMetrics(String metricExpression, String dataset) {
    MetricConfigManager metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    for (MetricConfigDTO metricConfig : metricConfigs) {
      if (metricConfig.isDerived()) {
        continue;
      }
      String metricName = metricConfig.getName();
      metricExpression = metricExpression.replaceAll(metricName,
          MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId());
    }
    return metricExpression;
  }

  public static String getDerivedMetricExpression(String metricExpressionName, String dataset) throws ExecutionException {
    String derivedMetricExpression = null;
    MetricDataset metricDataset = new MetricDataset(metricExpressionName, dataset);

    MetricConfigDTO metricConfig = CACHE_REGISTRY.getMetricConfigCache().get(metricDataset);

    if (metricConfig != null && metricConfig.isDerived()) {
      derivedMetricExpression = metricConfig.getDerivedMetricExpression();
    } else {
      derivedMetricExpression = MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId();
    }

    return derivedMetricExpression;
  }

  public static Map<String, Double> getMetricThresholdsMap(List<MetricFunction> metricFunctions) {
    Map<String, Double> metricThresholds = new HashMap<>();
    for (MetricFunction metricFunction : metricFunctions) {
      String derivedMetricExpression = metricFunction.getMetricName();
      String metricId = derivedMetricExpression.replaceAll(MetricConfigBean.DERIVED_METRIC_ID_PREFIX, "");
      MetricConfigDTO metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(Long.valueOf(metricId));
      metricThresholds.put(derivedMetricExpression, metricConfig.getRollupThreshold());
    }
    return metricThresholds;
  }

  public static String getMetricNameFromFunction(MetricFunction metricFunction) {
    String metricId = metricFunction.getMetricName().replace(MetricConfigBean.DERIVED_METRIC_ID_PREFIX, "");
    MetricConfigDTO metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(Long.valueOf(metricId));
    return metricConfig.getName();
  }

  public static String constructMetricAlias(String datasetName, String metricName) {
    String alias = datasetName + MetricConfigBean.ALIAS_JOINER + metricName;
    return alias;
  }

  public static Period getbaselineOffsetPeriodByMode(COMPARE_MODE compareMode) {
    int numWeeksAgo = 1;
    switch (compareMode) {
    case Wo2W:
      numWeeksAgo = 2;
      break;
    case Wo3W:
      numWeeksAgo = 3;
      break;
    case Wo4W:
      numWeeksAgo = 4;
      break;
    case WoW:
    default:
      numWeeksAgo = 1;
      break;
    }
    return new Period(0, 0, 0, 7 * numWeeksAgo, 0, 0, 0, 0);
  }

  public static DatasetConfigDTO getDatasetConfigFromName(String dataset) {
    DatasetConfigDTO datasetConfig = null;
    try {
      datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
    } catch (ExecutionException e) {
      LOG.error("Exception in getting dataset config {} from cache", dataset, e);
    }
    return datasetConfig;
  }

  public static List<DatasetConfigDTO> getDatasetConfigsFromMetricUrn(String metricUrn) {
    DatasetConfigManager datasetConfigManager = DAORegistry.getInstance().getDatasetConfigDAO();
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricConfigDTO metricConfig = DAORegistry.getInstance().getMetricConfigDAO().findById(me.getId());
    if (metricConfig == null) return new ArrayList<>();
    if (!metricConfig.isDerived()) {
      return Collections.singletonList(datasetConfigManager.findByDataset(metricConfig.getDataset()));
    } else {
      MetricExpression metricExpression = ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfig);
      List<MetricFunction> functions = metricExpression.computeMetricFunctions();
      return functions.stream().map(
          f -> datasetConfigManager.findByDataset(f.getDataset())).collect(Collectors.toList());
    }
  }

  /**
   * Get the expected delay for the detection pipeline.
   * This delay should be the longest of the expected delay of the underline datasets.
   *
   * @param config The detection config.
   * @return The expected delay for this alert in milliseconds.
   */
  public static long getDetectionExpectedDelay(DetectionConfigDTO config) {
    long maxExpectedDelay = 0;
    Set<String> metricUrns = DetectionConfigFormatter
        .extractMetricUrnsFromProperties(config.getProperties());
    for (String urn : metricUrns) {
      List<DatasetConfigDTO> datasets = ThirdEyeUtils.getDatasetConfigsFromMetricUrn(urn);
      for (DatasetConfigDTO dataset : datasets) {
        maxExpectedDelay = Math.max(dataset.getExpectedDelay().toMillis(), maxExpectedDelay);
      }
    }
    return maxExpectedDelay;
  }

  public static MetricConfigDTO getMetricConfigFromId(Long metricId) {
    MetricConfigDTO metricConfig = null;
    if (metricId != null) {
      metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
    }
    return metricConfig;
  }


  public static MetricConfigDTO getMetricConfigFromNameAndDataset(String metricName, String dataset) {
    MetricConfigDTO metricConfig = null;
    try {
      metricConfig = CACHE_REGISTRY.getMetricConfigCache().get(new MetricDataset(metricName, dataset));
    } catch (ExecutionException e) {
      LOG.error("Exception while fetching metric by name {} and dataset {}", metricName, dataset, e);
    }
    return metricConfig;
  }

  /**
   * Get rounded double value, according to the value of the double.
   * Max rounding will be up to 4 decimals
   * For values >= 0.1, use 2 decimals (eg. 123, 2.5, 1.26, 0.5, 0.162)
   * For values < 0.1, use 3 decimals (eg. 0.08, 0.071, 0.0123)
   * @param value any double value
   * @return the rounded double value
   */
  public static Double getRoundedDouble(Double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      return Double.NaN;
    }
    if (value >= 0.1) {
      return Math.round(value * (Math.pow(10, 2))) / (Math.pow(10, 2));
    } else {
      return Math.round(value * (Math.pow(10, 3))) / (Math.pow(10, 3));
    }
  }

  /**
   * Get rounded double value, according to the value of the double.
   * Max rounding will be upto 4 decimals
   * For values gte 0.1, use ##.## (eg. 123, 2.5, 1.26, 0.5, 0.162)
   * For values lt 0.1 and gte 0.01, use ##.### (eg. 0.08, 0.071, 0.0123)
   * For values lt 0.01 and gte 0.001, use ##.#### (eg. 0.001, 0.00367)
   * This function ensures we don't prematurely round off double values to a fixed format, and make it 0.00 or lose out information
   * @param value
   * @return
   */
  public static String getRoundedValue(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      if (Double.isNaN(value)) {
        return Double.toString(Double.NaN);
      }
      if (value > 0) {
        return Double.toString(Double.POSITIVE_INFINITY);
      } else {
        return Double.toString(Double.NEGATIVE_INFINITY);
      }
    }
    StringBuffer decimalFormatBuffer = new StringBuffer(TWO_DECIMALS_FORMAT);
    double compareValue = 0.1;
    while (value > 0 && value < compareValue && !decimalFormatBuffer.toString().equals(MAX_DECIMALS_FORMAT)) {
      decimalFormatBuffer.append(DECIMALS_FORMAT_TOKEN);
      compareValue = compareValue * 0.1;
    }
    DecimalFormat decimalFormat = new DecimalFormat(decimalFormatBuffer.toString());

    return decimalFormat.format(value);
  }


  //TODO: currently assuming all metrics in one request are for the same data source
  // It would be better to not assume that, and split the thirdeye request into more requests depending upon the data sources
  public static String getDataSourceFromMetricFunctions(List<MetricFunction> metricFunctions) {
    String dataSource = null;
    for (MetricFunction metricFunction : metricFunctions) {
      String functionDatasetDatasource = metricFunction.getDatasetConfig().getDataSource();
      if (dataSource == null) {
        dataSource = functionDatasetDatasource;
      } else if (!dataSource.equals(functionDatasetDatasource)) {
        throw new IllegalStateException("All metric funcitons of one request must belong to the same data source. "
            + dataSource + " is not equal to" + functionDatasetDatasource);
      }
    }
    return dataSource;
  }

  /**
   * Converts a Properties, which is a Map<Object, Object>, to a Map<String, String>.
   *
   * @param properties the properties to be converted
   * @return the map of the properties.
   */
  public static Map<String, String> propertiesToStringMap(Properties properties) {
    Map<String, String> map = new HashMap<>();
    if (MapUtils.isNotEmpty(properties)) {
      for (String propertyKey : properties.stringPropertyNames()) {
        map.put(propertyKey, properties.getProperty(propertyKey));
      }
    }
    return map;
  }

  /**
   * Prints messages and stack traces of the given list of exceptions in a string.
   *
   * @param exceptions the list of exceptions to be printed.
   * @param maxWordCount the length limitation of the string; set to 0 to remove the limitation.
   *
   * @return the string that contains the messages and stack traces of the given exceptions.
   */
  public static String exceptionsToString(List<Exception> exceptions, int maxWordCount) {
    String message = "";
    if (CollectionUtils.isNotEmpty(exceptions)) {
      StringBuilder sb = new StringBuilder();
      for (Exception exception : exceptions) {
        sb.append(ExceptionUtils.getStackTrace(exception));
        if (maxWordCount > 0 && sb.length() > maxWordCount) {
          message = sb.toString().substring(0, maxWordCount) + "\n...";
          break;
        }
      }
      if (message.equals("")) {
        message = sb.toString();
      }
    }
    return message;
  }

  /**
   * Initializes a light weight ThirdEye environment for conducting standalone experiments.
   *
   * @param thirdEyeConfigDir the directory to ThirdEye configurations.
   */
  public static void initLightWeightThirdEyeEnvironment(String thirdEyeConfigDir) {
    System.setProperty("dw.rootDir", thirdEyeConfigDir);

    // Initialize DAO Registry
    String persistenceConfig = thirdEyeConfigDir + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    DaoProviderUtil.init(new File(persistenceConfig));

    // Read configuration for data sources, etc.
    // TODO spyne Fix dependency loading
    ThirdEyeConfiguration config;
    try {
      String dashboardConfigFilePath = thirdEyeConfigDir + "/dashboard.yml";
      File configFile = new File(dashboardConfigFilePath);
      YamlConfigurationFactory<ThirdEyeConfiguration> factory =
          new YamlConfigurationFactory<>(ThirdEyeConfiguration.class,
              Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(), "");
      config = factory.build(configFile);
      config.setRootDir(thirdEyeConfigDir);
    } catch (Exception e) {
      LOG.error("Exception while constructing ThirdEye config:", e);
      throw new RuntimeException(e);
    }

    // Initialize Cache Registry
    try {
      ThirdEyeCacheRegistry.initializeCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches:", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Guess time duration from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  public static int getTimeDuration(Period granularity) {
    if (granularity.getDays() > 0) {
      return granularity.getDays();
    }
    if (granularity.getHours() > 0) {
      return granularity.getHours();
    }
    if (granularity.getMinutes() > 0) {
      return granularity.getMinutes();
    }
    if (granularity.getSeconds() > 0) {
      return granularity.getSeconds();
    }
    return granularity.getMillis();
  }

  /**
   * Guess time unit from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  public static TimeUnit getTimeUnit(Period granularity) {
    if (granularity.getDays() > 0) {
      return TimeUnit.DAYS;
    }
    if (granularity.getHours() > 0) {
      return TimeUnit.HOURS;
    }
    if (granularity.getMinutes() > 0) {
      return TimeUnit.MINUTES;
    }
    if (granularity.getSeconds() > 0) {
      return TimeUnit.SECONDS;
    }
    return TimeUnit.MILLISECONDS;
  }

  public static LoadingCache<RelationalQuery, ThirdEyeResultSetGroup> buildResponseCache(
      CacheLoader cacheLoader) throws Exception {
    Preconditions.checkNotNull(cacheLoader, "A cache loader is required.");

    // Initializes listener that prints expired entries in debuggin mode.
    RemovalListener<RelationalQuery, ThirdEyeResultSet> listener;
    if (LOG.isDebugEnabled()) {
      listener = new RemovalListener<RelationalQuery, ThirdEyeResultSet>() {
        @Override
        public void onRemoval(RemovalNotification<RelationalQuery, ThirdEyeResultSet> notification) {
          LOG.debug("Expired {}", notification.getKey().getQuery());
        }
      };
    } else {
      listener = new RemovalListener<RelationalQuery, ThirdEyeResultSet>() {
        @Override public void onRemoval(RemovalNotification<RelationalQuery, ThirdEyeResultSet> notification) { }
      };
    }

    // ResultSetGroup Cache. The size of this cache is limited by the total number of buckets in all ResultSetGroup.
    // We estimate that 1 bucket (including overhead) consumes 1KB and this cache is allowed to use up to 50% of max
    // heap space.
    long maxBucketNumber = getApproximateMaxBucketNumber(DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE);
    LOG.debug("Max bucket number for {}'s cache is set to {}", cacheLoader.toString(), maxBucketNumber);

    return CacheBuilder.newBuilder()
        .removalListener(listener)
        .expireAfterWrite(15, TimeUnit.MINUTES)
        .maximumWeight(maxBucketNumber)
        .weigher(new Weigher<RelationalQuery, ThirdEyeResultSetGroup>() {
          @Override public int weigh(RelationalQuery relationalQuery, ThirdEyeResultSetGroup resultSetGroup) {
            int resultSetCount = resultSetGroup.size();
            int weight = 0;
            for (int idx = 0; idx < resultSetCount; ++idx) {
              ThirdEyeResultSet resultSet = resultSetGroup.get(idx);
              weight += ((resultSet.getColumnCount() + resultSet.getGroupKeyLength()) * resultSet.getRowCount());
            }
            return weight;
          }
        })
        .build(cacheLoader);
  }

  private static long getApproximateMaxBucketNumber(int percentage) {
    long jvmMaxMemoryInBytes = Runtime.getRuntime().maxMemory();
    if (jvmMaxMemoryInBytes == Long.MAX_VALUE) { // Check upper bound
      jvmMaxMemoryInBytes = DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * FileUtils.ONE_MB; // MB to Bytes
    } else { // Check lower bound
      long lowerBoundInBytes = DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * FileUtils.ONE_MB; // MB to Bytes
      if (jvmMaxMemoryInBytes < lowerBoundInBytes) {
        jvmMaxMemoryInBytes = lowerBoundInBytes;
      }
    }
    return (jvmMaxMemoryInBytes / 102400) * percentage;
  }


  /**
   * Merge child's properties into parent's properties.
   * If the property exists in both then use parent's property.
   * For property = "detectorComponentName", combine the parent and child.
   * @param parent The parent anomaly's properties.
   * @param child The child anomaly's properties.
   */
  public static void mergeAnomalyProperties(Map<String, String> parent, Map<String, String> child) {
    for (String key : child.keySet()) {
      if (!parent.containsKey(key)) {
        parent.put(key, child.get(key));
      } else {
        // combine detectorComponentName
        if (key.equals(PROP_DETECTOR_COMPONENT_NAME)) {
          String component = ThirdEyeUtils.combineComponents(parent.get(PROP_DETECTOR_COMPONENT_NAME), child.get(PROP_DETECTOR_COMPONENT_NAME));
          parent.put(PROP_DETECTOR_COMPONENT_NAME, component);
        }
        // combine time series snapshot of parent and child anomalies
        if (key.equals(MergedAnomalyResultDTO.TIME_SERIES_SNAPSHOT_KEY)) {
          try {
            AnomalyTimelinesView parentTimeSeries = AnomalyTimelinesView
                .fromJsonString(parent.get(TIME_SERIES_SNAPSHOT_KEY));
            AnomalyTimelinesView childTimeSeries = AnomalyTimelinesView
                .fromJsonString(child.get(TIME_SERIES_SNAPSHOT_KEY));
            parent.put(TIME_SERIES_SNAPSHOT_KEY,
                mergeTimeSeriesSnapshot(parentTimeSeries, childTimeSeries).toJsonString());
          } catch (Exception e) {
            LOG.warn("Unable to merge time series, so skipping...", e);
          }
        }
      }
    }
  }

  public static long getCachingPeriodLookback(TimeGranularity granularity) {
    long period;
    switch (granularity.getUnit()) {
      case DAYS:
        // 90 days data for daily detection
        period = CACHING_PERIOD_LOOKBACK_DAILY;
        break;
      case HOURS:
        // 60 days data for hourly detection
        period = CACHING_PERIOD_LOOKBACK_HOURLY;
        break;
      case MINUTES:
        // disable minute level cache warmup by default.
        period = CACHING_PERIOD_LOOKBACK_MINUTELY;
        break;
      default:
        period = DEFAULT_CACHING_PERIOD_LOOKBACK;
    }
    return period;
  }

  /**
   * Check if the anomaly is detected by multiple components
   * @param anomaly the anomaly
   * @return if the anomaly is detected by multiple components
   */
  public static boolean isDetectedByMultipleComponents(MergedAnomalyResultDTO anomaly) {
    String componentName = anomaly.getProperties().getOrDefault(PROP_DETECTOR_COMPONENT_NAME, "");
    return componentName.contains(PROP_DETECTOR_COMPONENT_NAME_DELIMETER);
  }

  /**
   * Combine two components with comma separated.
   * For example, will combine "component1" and "component2" into "component1, component2".
   *
   * @param component1 The first component.
   * @param component2 The second component.
   * @return The combined components.
   */
  private static String combineComponents(String component1, String component2) {
    List<String> components = new ArrayList<>();
    components.addAll(Arrays.asList(component1.split(PROP_DETECTOR_COMPONENT_NAME_DELIMETER)));
    components.addAll(Arrays.asList(component2.split(PROP_DETECTOR_COMPONENT_NAME_DELIMETER)));
    return components.stream().distinct().collect(Collectors.joining(PROP_DETECTOR_COMPONENT_NAME_DELIMETER));
  }

  /**
   * Parse job name to get the detection id
   * @param jobName
   * @return
   */
  public static long getDetectionIdFromJobName(String jobName) {
    String[] parts = jobName.split("_");
    if (parts.length < 2) {
      throw new IllegalArgumentException("Invalid job name: " + jobName);
    }
    return Long.parseLong(parts[1]);
  }

  /**
   * A helper function to merge time series snapshot of two anomalies. This function assumes that the time series of
   * both parent and child anomalies are aligned with the metric granularity boundary.
   * @param parent time series snapshot of parent anomaly
   * @param child time series snapshot of parent anaomaly
   * @return merged time series snapshot based on timestamps
   */
  private static AnomalyTimelinesView mergeTimeSeriesSnapshot(AnomalyTimelinesView parent, AnomalyTimelinesView child) {
    AnomalyTimelinesView mergedTimeSeriesSnapshot = new AnomalyTimelinesView();
    int i = 0 , j = 0;
    while(i < parent.getTimeBuckets().size() && j < child.getTimeBuckets().size()) {
      long parentTime = parent.getTimeBuckets().get(i).getCurrentStart();
      long childTime = child.getTimeBuckets().get(j).getCurrentStart();
      if (parentTime == childTime) {
        // use the values in parent anomalies when the time series overlap
        mergedTimeSeriesSnapshot.addTimeBuckets(parent.getTimeBuckets().get(i));
        mergedTimeSeriesSnapshot.addCurrentValues(parent.getCurrentValues().get(i));
        mergedTimeSeriesSnapshot.addBaselineValues(parent.getBaselineValues().get(i));
        i++;
        j++;
      } else if (parentTime < childTime) {
        mergedTimeSeriesSnapshot.addTimeBuckets(parent.getTimeBuckets().get(i));
        mergedTimeSeriesSnapshot.addCurrentValues(parent.getCurrentValues().get(i));
        mergedTimeSeriesSnapshot.addBaselineValues(parent.getBaselineValues().get(i));
        i++;
      } else {
        mergedTimeSeriesSnapshot.addTimeBuckets(child.getTimeBuckets().get(j));
        mergedTimeSeriesSnapshot.addCurrentValues(child.getCurrentValues().get(j));
        mergedTimeSeriesSnapshot.addBaselineValues(child.getBaselineValues().get(j));
        j++;
      }
    }
    while (i < parent.getTimeBuckets().size()) {
      mergedTimeSeriesSnapshot.addTimeBuckets(parent.getTimeBuckets().get(i));
      mergedTimeSeriesSnapshot.addCurrentValues(parent.getCurrentValues().get(i));
      mergedTimeSeriesSnapshot.addBaselineValues(parent.getBaselineValues().get(i));
      i++;
    }
    while (j < child.getTimeBuckets().size()) {
      mergedTimeSeriesSnapshot.addTimeBuckets(child.getTimeBuckets().get(j));
      mergedTimeSeriesSnapshot.addCurrentValues(child.getCurrentValues().get(j));
      mergedTimeSeriesSnapshot.addBaselineValues(child.getBaselineValues().get(j));
      j++;
    }
    mergedTimeSeriesSnapshot.getSummary().putAll(parent.getSummary());
    for (String key : child.getSummary().keySet()) {
      if (!mergedTimeSeriesSnapshot.getSummary().containsKey(key)) {
        mergedTimeSeriesSnapshot.getSummary().put(key, child.getSummary().get(key));
      }
    }
    return mergedTimeSeriesSnapshot;
  }
}
