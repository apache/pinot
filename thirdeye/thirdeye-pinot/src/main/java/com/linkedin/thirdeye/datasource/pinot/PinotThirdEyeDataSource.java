package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class PinotThirdEyeDataSource implements ThirdEyeDataSource {

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeDataSource.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final HttpHost controllerHost;
  private final CloseableHttpClient controllerClient;
  public static final String DATA_SOURCE_NAME = PinotThirdEyeDataSource.class.getSimpleName();
  private PinotDataSourceMaxTime pinotDataSourceMaxTime;
  private PinotDataSourceDimensionFilters pinotDataSourceDimensionFilters;

  public PinotThirdEyeDataSource(Map<String, String> properties) {
    if (!isValidProperties(properties)) {
      throw new IllegalStateException("Invalid properties for data source " + DATA_SOURCE_NAME + " " + properties);
    }
    String host = properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
    int port = Integer.valueOf(properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue()));
    String zookeeperUrl = properties.get(PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());

    ZkClient zkClient = new ZkClient(zookeeperUrl);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected();
    this.controllerHost = new HttpHost(host, port);
    this.controllerClient = HttpClients.createDefault();

    pinotDataSourceMaxTime = new PinotDataSourceMaxTime();
    pinotDataSourceDimensionFilters = new PinotDataSourceDimensionFilters();
    LOG.info("Created PinotThirdEyeDataSource with controller {}", controllerHost);
  }

  protected PinotThirdEyeDataSource(String host, int port) {
    this.controllerHost = new HttpHost(host, port);
    this.controllerClient = HttpClients.createDefault();
    pinotDataSourceMaxTime = new PinotDataSourceMaxTime();
    pinotDataSourceDimensionFilters = new PinotDataSourceDimensionFilters();
    LOG.info("Created PinotThirdEyeDataSource with controller {}", controllerHost);
  }

  public static PinotThirdEyeDataSource fromZookeeper(String controllerHost, int controllerPort, String zkUrl) {
    ZkClient zkClient = new ZkClient(zkUrl);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected();
    PinotThirdEyeDataSource pinotThirdEyeDataSource = new PinotThirdEyeDataSource(controllerHost, controllerPort);
    LOG.info("Created PinotThirdEyeDataSource to zookeeper: {} controller: {}:{}", zkUrl, controllerHost, controllerPort);
    return pinotThirdEyeDataSource;
  }

  public static ThirdEyeDataSource fromDataSourceConfig(PinotThirdEyeDataSourceConfig pinotDataSourceConfig) {
    return fromZookeeper(pinotDataSourceConfig.getControllerHost(), pinotDataSourceConfig.getControllerPort(), pinotDataSourceConfig.getZookeeperUrl());
  }


  @Override
  public String getName() {
    return DATA_SOURCE_NAME;
  }

  @Override
  public PinotThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    long tStart = System.nanoTime();
    try {
      LinkedHashMap<MetricFunction, List<ResultSet>> metricFunctionToResultSetList = new LinkedHashMap<>();

      TimeSpec timeSpec = null;
      for (MetricFunction metricFunction : request.getMetricFunctions()) {
        String dataset = metricFunction.getDataset();
        DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
        TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
        if (timeSpec == null) {
          timeSpec = dataTimeSpec;
        }

        // Decorate filter set for pre-computed (non-additive) dataset
        Multimap<String, String> decoratedFilterSet = request.getFilterSet();
        if (!datasetConfig.isAdditive()) {
          decoratedFilterSet =
              generateFilterSetWithPreAggregatedDimensionValue(request.getFilterSet(), request.getGroupBy(),
                  datasetConfig.getDimensions(), datasetConfig.getDimensionsHaveNoPreAggregation(),
                  datasetConfig.getPreAggregatedKeyword());
        }

        // By default, query only offline, unless dataset has been marked as realtime
        String tableName = ThirdEyeUtils.computeTableName(dataset);
        String pql = null;
        if (datasetConfig.isMetricAsDimension()) {
          pql = PqlUtils.getMetricAsDimensionPql(request, metricFunction, decoratedFilterSet, dataTimeSpec, datasetConfig);
        } else {
          pql = PqlUtils.getPql(request, metricFunction, decoratedFilterSet, dataTimeSpec);
        }
        ResultSetGroup resultSetGroup = CACHE_REGISTRY_INSTANCE.getResultSetGroupCache().get(new PinotQuery(pql, tableName));
        metricFunctionToResultSetList.put(metricFunction, getResultSetList(resultSetGroup));
      }

      List<String[]> resultRows = parseResultSets(request, metricFunctionToResultSetList);
      PinotThirdEyeResponse resp = new PinotThirdEyeResponse(request, resultRows, timeSpec);
      return resp;
    } finally {
      ThirdeyeMetricsUtil.pinotCallCounter.inc();
      ThirdeyeMetricsUtil.pinotDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Definition of Pre-Aggregated Data: the data that has been pre-aggregated or pre-calculated and should not be
   * applied with any aggregation function during grouping by. Usually, this kind of data exists in non-additive
   * dataset. For such data, we assume that there exists a dimension value named "all", which could be overridden
   * in dataset configuration, that stores the pre-aggregated value.
   *
   * By default, when a query does not specify any value on pre-aggregated dimension, Pinot aggregates all values
   * at that dimension, which is an undesirable behavior for non-additive data. Therefore, this method modifies the
   * request's dimension filters such that the filter could pick out the "all" value for that dimension. Example:
   * Suppose that we have a dataset with 3 pre-aggregated dimensions: country, pageName, and osName, and the pre-
   * aggregated keyword is 'all'. Further assume that the original request's filter = {'country'='US, IN'} and
   * GroupBy dimension = pageName, then the decorated request has the new filter =
   * {'country'='US, IN', 'osName' = 'all'}. Note that 'pageName' = 'all' is not in the filter set because it is
   * a GroupBy dimension, which will not be aggregated.
   *
   * @param filterSet the original filterSet, which will NOT be modified.
   *
   * @return a decorated filter set for the queries to the pre-aggregated dataset.
   */
  public static Multimap<String, String> generateFilterSetWithPreAggregatedDimensionValue(
      Multimap<String, String> filterSet, List<String> groupByDimensions, List<String> allDimensions,
      List<String> dimensionsHaveNoPreAggregation, String preAggregatedKeyword) {

    Set<String> preAggregatedDimensionNames = new HashSet<>(allDimensions);
    // Remove dimension names that do not have the pre-aggregated value
    if (CollectionUtils.isNotEmpty(dimensionsHaveNoPreAggregation)) {
      preAggregatedDimensionNames.removeAll(dimensionsHaveNoPreAggregation);
    }
    // Remove dimension names that have been included in the original filter set because we should not override
    // users' explicit filter setting
    if (filterSet != null) {
      preAggregatedDimensionNames.removeAll(filterSet.asMap().keySet());
    }
    // Remove dimension names that are going to be grouped by because GroupBy dimensions will not be aggregated anyway
    if (CollectionUtils.isNotEmpty(groupByDimensions)) {
      preAggregatedDimensionNames.removeAll(groupByDimensions);
    }
    // Add pre-aggregated dimension value to the remaining dimension names
    Multimap<String, String> decoratedFilterSet;
    if (filterSet != null) {
      decoratedFilterSet = HashMultimap.create(filterSet);
    } else {
      decoratedFilterSet = HashMultimap.create();
    }
    if (preAggregatedDimensionNames.size() != 0) {
      for (String preComputedDimensionName : preAggregatedDimensionNames) {
        decoratedFilterSet.put(preComputedDimensionName, preAggregatedKeyword);
      }
    }

    return decoratedFilterSet;
  }

  private static List<ResultSet> getResultSetList(ResultSetGroup resultSetGroup) {
    List<ResultSet> resultSets = new ArrayList<>();
    for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
        resultSets.add(resultSetGroup.getResultSet(i));
    }
    return resultSets;
  }


  private List<String[]> parseResultSets(ThirdEyeRequest request,
      Map<MetricFunction, List<ResultSet>> metricFunctionToResultSetList) throws ExecutionException {

    int numGroupByKeys = 0;
    boolean hasGroupBy = false;
    if (request.getGroupByTimeGranularity() != null) {
      numGroupByKeys += 1;
    }
    if (request.getGroupBy() != null) {
      numGroupByKeys += request.getGroupBy().size();
    }
    if (numGroupByKeys > 0) {
      hasGroupBy = true;
    }
    int numMetrics = request.getMetricFunctions().size();
    int numCols = numGroupByKeys + numMetrics;
    boolean hasGroupByTime = false;
    if (request.getGroupByTimeGranularity() != null) {
      hasGroupByTime = true;
    }

    int position = 0;
    LinkedHashMap<String, String[]> dataMap = new LinkedHashMap<>();
    for (Entry<MetricFunction, List<ResultSet>> entry : metricFunctionToResultSetList.entrySet()) {

      MetricFunction metricFunction = entry.getKey();

      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

      TimeGranularity dataGranularity = null;
      long startTime = request.getStartTimeInclusive().getMillis();
      DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
      DateTime startDateTime = new DateTime(startTime, dateTimeZone);
      dataGranularity = dataTimeSpec.getDataGranularity();
      boolean isISOFormat = false;
      DateTimeFormatter inputDataDateTimeFormatter = null;
      String timeFormat = dataTimeSpec.getFormat();
      if (timeFormat != null && !timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
        isISOFormat = true;
        inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dateTimeZone);
      }

      List<ResultSet> resultSets = entry.getValue();
      for (int i = 0; i < resultSets.size(); i++) {
        ResultSet resultSet = resultSets.get(i);
        int numRows = resultSet.getRowCount();
        for (int r = 0; r < numRows; r++) {
          boolean skipRowDueToError = false;
          String[] groupKeys;
          if (hasGroupBy) {
            groupKeys = new String[resultSet.getGroupKeyLength()];
            for (int grpKeyIdx = 0; grpKeyIdx < resultSet.getGroupKeyLength(); grpKeyIdx++) {
              String groupKeyVal = "";
              try {
                groupKeyVal = resultSet.getGroupKeyString(r, grpKeyIdx);
              } catch (Exception e) {
                // IGNORE FOR NOW, workaround for Pinot Bug
              }
              if (hasGroupByTime && grpKeyIdx == 0) {
                int timeBucket;
                long millis;
                if (!isISOFormat) {
                  millis = dataGranularity.toMillis(Double.valueOf(groupKeyVal).longValue());
                } else {
                  millis = DateTime.parse(groupKeyVal, inputDataDateTimeFormatter).getMillis();
                }
                if (millis < startTime) {
                  LOG.error("Data point earlier than requested start time {}: {}", new Date(startTime), new Date(millis));
                  skipRowDueToError = true;
                  break;
                }
                timeBucket = TimeRangeUtils
                    .computeBucketIndex(request.getGroupByTimeGranularity(), startDateTime,
                        new DateTime(millis, dateTimeZone));
                groupKeyVal = String.valueOf(timeBucket);
              }
              groupKeys[grpKeyIdx] = groupKeyVal;
            }
            if (skipRowDueToError) {
              continue;
            }
          } else {
            groupKeys = new String[] {};
          }
          StringBuilder groupKeyBuilder = new StringBuilder("");
          for (String grpKey : groupKeys) {
            groupKeyBuilder.append(grpKey).append("|");
          }
          String compositeGroupKey = groupKeyBuilder.toString();
          String[] rowValues = dataMap.get(compositeGroupKey);
          if (rowValues == null) {
            rowValues = new String[numCols];
            Arrays.fill(rowValues, "0");
            System.arraycopy(groupKeys, 0, rowValues, 0, groupKeys.length);
            dataMap.put(compositeGroupKey, rowValues);
          }
          rowValues[groupKeys.length + position + i] =
              String.valueOf(Double.parseDouble(rowValues[groupKeys.length + position + i])
                  + Double.parseDouble(resultSet.getString(r, 0)));
        }
      }
      position ++;
    }
    List<String[]> rows = new ArrayList<>();
    rows.addAll(dataMap.values());
    return rows;

  }


  @Override
  public List<String> getDatasets() throws Exception {
    return CACHE_REGISTRY_INSTANCE.getDatasetsCache().getDatasets();
  }


  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    return pinotDataSourceMaxTime.getMaxDateTime(dataset);
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return pinotDataSourceDimensionFilters.getDimensionFilters(dataset);
  }

  @Override
  public void clear() throws Exception {
  }

  @Override
  public void close() throws Exception {
    controllerClient.close();
  }

  private boolean isValidProperties(Map<String, String> properties) {
    boolean valid = true;
    if (MapUtils.isEmpty(properties)) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing properties {}", properties);
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue());
    }
    return valid;
  }

  /** TESTING ONLY - WE SHOULD NOT BE USING THIS. */
  @Deprecated
  public static PinotThirdEyeDataSource getDefaultTestDataSource() {
    // TODO REPLACE WITH CONFIGS
    String controllerHost = "localhost";
    int controllerPort = 11984;
    String zkUrl =
        "localhost:12913/pinot-cluster";
    return fromZookeeper(controllerHost, controllerPort, zkUrl);
  }


}
