/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.linkedin.thirdeye.util;

import com.google.common.collect.HashMultimap;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.api.DimensionMap;

import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.validation.Validation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.COMPARE_MODE;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.MetricDataset;


public abstract class ThirdEyeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeUtils.class);
  private static final String FILTER_VALUE_ASSIGNMENT_SEPARATOR = "=";
  private static final String FILTER_CLAUSE_SEPARATOR = ";";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();
  private static final String TWO_DECIMALS_FORMAT = "##.##";
  private static final String MAX_DECIMALS_FORMAT = "##.#####";
  private static final String DECIMALS_FORMAT_TOKEN = "#";

  private ThirdEyeUtils () {

  }

  public static Multimap<String, String> getFilterSet(String filters) {
    Multimap<String, String> filterSet = ArrayListMultimap.create();
    if (StringUtils.isNotBlank(filters)) {
      String[] filterClauses = filters.split(FILTER_CLAUSE_SEPARATOR);
      for (String filterClause : filterClauses) {
        String[] values = filterClause.split(FILTER_VALUE_ASSIGNMENT_SEPARATOR, 2);
        if (values.length != 2) {
          throw new IllegalArgumentException("Filter values assigments should in pairs: " + filters);
        }
        filterSet.put(values[0], values[1]);
      }
    }
    return filterSet;
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

  public static String getSortedFiltersFromMultiMap(Multimap<String, String> filterMultiMap) {
    Set<String> filterKeySet = filterMultiMap.keySet();
    ArrayList<String> filterKeyList = new ArrayList<String>(filterKeySet);
    Collections.sort(filterKeyList);

    StringBuilder sb = new StringBuilder();
    for (String filterKey : filterKeyList) {
      ArrayList<String> values = new ArrayList<String>(filterMultiMap.get(filterKey));
      Collections.sort(values);
      for (String value : values) {
        sb.append(filterKey);
        sb.append(FILTER_VALUE_ASSIGNMENT_SEPARATOR);
        sb.append(value);
        sb.append(FILTER_CLAUSE_SEPARATOR);
      }
    }
    return StringUtils.chop(sb.toString());
  }

  public static String getSortedFilters(String filters) {
    Multimap<String, String> filterMultiMap = getFilterSet(filters);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }
    return sortedFilters;
  }

  public static String getSortedFiltersFromJson(String filterJson) {
    Multimap<String, String> filterMultiMap = convertToMultiMap(filterJson);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }
    return sortedFilters;
  }

  private static String getTimeFormatString(DatasetConfigDTO datasetConfig) {
    String timeFormat = datasetConfig.getTimeFormat();
    if (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
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


  //By default, query only offline, unless dataset has been marked as realtime
  public static String computeTableName(String collection) {
    String dataset = null;
    try {
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(collection);
      dataset = collection + DatasetConfigBean.DATASET_OFFLINE_PREFIX;
      if (datasetConfig.isRealtime()) {
        dataset = collection;
      }
    } catch (ExecutionException e) {
      LOG.error("Exception in getting dataset name {}", collection, e);
    }
    return dataset;
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

  public static String getDatasetFromMetricFunction(MetricFunction metricFunction) {
    MetricConfigDTO metricConfig = getMetricConfigFromId(metricFunction.getMetricId());
    return metricConfig.getDataset();
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
    ThirdEyeDashboardConfiguration config;
    try {
      String dashboardConfigFilePath = thirdEyeConfigDir + "/dashboard.yml";
      File configFile = new File(dashboardConfigFilePath);
      ConfigurationFactory<ThirdEyeDashboardConfiguration> factory =
          new ConfigurationFactory<>(ThirdEyeDashboardConfiguration.class,
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
}
