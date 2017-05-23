package com.linkedin.thirdeye.datasource.pinot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

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

    LinkedHashMap<MetricFunction, List<ResultSet>> metricFunctionToResultSetList = new LinkedHashMap<>();

    TimeSpec timeSpec = null;
    for (MetricFunction metricFunction : request.getMetricFunctions()) {
      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
      if (timeSpec == null) {
        timeSpec = dataTimeSpec;
      }

      // By default, query only offline, unless dataset has been marked as realtime
      String tableName = ThirdEyeUtils.computeTableName(dataset);
      String pql = null;
      if (datasetConfig.isMetricAsDimension()) {
        pql = PqlUtils.getMetricAsDimensionPql(request, metricFunction, dataTimeSpec, datasetConfig);
      } else {
        pql = PqlUtils.getPql(request, metricFunction, dataTimeSpec);
      }
      ResultSetGroup resultSetGroup = CACHE_REGISTRY_INSTANCE.getResultSetGroupCache().get(new PinotQuery(pql, tableName));
      metricFunctionToResultSetList.put(metricFunction, getResultSetList(resultSetGroup));
    }

    List<String[]> resultRows = parseResultSets(request, metricFunctionToResultSetList);
    PinotThirdEyeResponse resp = new PinotThirdEyeResponse(request, resultRows, timeSpec);
    return resp;
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
