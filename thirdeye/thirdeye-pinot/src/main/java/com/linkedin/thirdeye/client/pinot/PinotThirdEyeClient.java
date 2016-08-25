package com.linkedin.thirdeye.client.pinot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

public class PinotThirdEyeClient implements ThirdEyeClient {

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeClient.class);

  private static final ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  public static final String CONTROLLER_HOST_PROPERTY_KEY = "controllerHost";
  public static final String CONTROLLER_PORT_PROPERTY_KEY = "controllerPort";
  public static final String FIXED_COLLECTIONS_PROPERTY_KEY = "fixedCollections";
  public static final String CLUSTER_NAME_PROPERTY_KEY = "clusterName";
  public static final String TAG_PROPERTY_KEY = "tag";

  String segementZKMetadataRootPath;
  private final HttpHost controllerHost;
  private final CloseableHttpClient controllerClient;

  protected PinotThirdEyeClient(String controllerHostName, int controllerPort) {

    this.controllerHost = new HttpHost(controllerHostName, controllerPort);
    // TODO currently no way to configure the CloseableHttpClient
    this.controllerClient = HttpClients.createDefault();
    LOG.info("Created PinotThirdEyeClient with controller {}", controllerHost);
  }

  /* Static builder methods to mirror Pinot Java API (ConnectionFactory) */
  public static PinotThirdEyeClient fromHostList(String controllerHost, int controllerPort,
      String... brokers) {
    if (brokers == null || brokers.length == 0) {
      throw new IllegalArgumentException("Please specify at least one broker.");
    }
    LOG.info("Created PinotThirdEyeClient to hosts: {}", (Object[]) brokers);
    return new PinotThirdEyeClient(controllerHost, controllerPort);
  }

  /**
   * Creates a new PinotThirdEyeClient using the clusterName and broker tag
   * @param config
   * @param controllerHost
   * @param controllerPort
   * @param zkUrl
   * @param clusterName : required property
   * @param tag : required property
   * @return
   */
  public static PinotThirdEyeClient fromZookeeper(String controllerHost, int controllerPort,
      String zkUrl, String clusterName) {
    ZkClient zkClient = new ZkClient(zkUrl);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected();
    PinotThirdEyeClient pinotThirdEyeClient =
        new PinotThirdEyeClient(controllerHost, controllerPort);
    LOG.info("Created PinotThirdEyeClient to zookeeper: {} controller: {}:{}", zkUrl,
        controllerHost, controllerPort);
    return pinotThirdEyeClient;
  }

  public static PinotThirdEyeClient fromProperties(Properties properties) {
    LOG.info("Created PinotThirdEyeClient from properties {}", properties);
    if (!properties.containsKey(CONTROLLER_HOST_PROPERTY_KEY)
        || !properties.containsKey(CONTROLLER_PORT_PROPERTY_KEY)) {
      throw new IllegalArgumentException("Properties file must contain controller mappings for "
          + CONTROLLER_HOST_PROPERTY_KEY + " and " + CONTROLLER_PORT_PROPERTY_KEY);
    }
    return new PinotThirdEyeClient(properties.getProperty(CONTROLLER_HOST_PROPERTY_KEY),
        Integer.valueOf(properties.getProperty(CONTROLLER_PORT_PROPERTY_KEY)));
  }

  public static ThirdEyeClient fromClientConfig(PinotThirdEyeClientConfig config) {
    if (config.getBrokerUrl() != null && config.getBrokerUrl().trim().length() > 0) {
      return fromHostList(config.getControllerHost(), config.getControllerPort(), config.brokerUrl);
    }
    return fromZookeeper(config.getControllerHost(), config.getControllerPort(),
        config.getZookeeperUrl(), config.getClusterName());
  }

  @Override
  public PinotThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    CollectionSchema collectionSchema =
        CACHE_INSTANCE.getCollectionSchemaCache().get(request.getCollection());
    TimeSpec dataTimeSpec = collectionSchema.getTime();
    List<MetricFunction> metricFunctions = request.getMetricFunctions();
    List<String> dimensionNames = collectionSchema.getDimensionNames();
    CollectionConfig collectionConfig = null;
    List<ResultSetGroup> resultSetGroups = new ArrayList<>();

    try {
      collectionConfig = CACHE_INSTANCE.getCollectionConfigCache().get(request.getCollection());
    } catch (Exception e) {
      LOG.debug("Collection config for collection {} does not exist", request.getCollection());
    }
    if (collectionConfig != null && collectionConfig.isMetricAsDimension()) {
       List<String> pqls = PqlUtils.getMetricAsDimensionPqls(request, dataTimeSpec, collectionConfig);
       for (String pql : pqls) {
         ResultSetGroup result = CACHE_INSTANCE.getResultSetGroupCache()
             .get(new PinotQuery(pql, request.getCollection()));
         resultSetGroups.add(result);
       }
    } else {
      String sql = PqlUtils.getPql(request, dataTimeSpec);
      LOG.debug("Executing: {}", sql);
      ResultSetGroup result = CACHE_INSTANCE.getResultSetGroupCache()
          .get(new PinotQuery(sql, request.getCollection()));
      resultSetGroups.add(result);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Result for: {} {}", sql, format(result));
      }
    }
    List<ResultSet> resultSets = getResultSets(resultSetGroups);
    List<String[]> resultRows = parseResultSets(request, resultSets, metricFunctions, collectionSchema, dimensionNames);
    PinotThirdEyeResponse resp = new PinotThirdEyeResponse(request, resultRows, dataTimeSpec);
    return resp;

  }

  private static List<ResultSet> getResultSets(List<ResultSetGroup> resultSetGroups) {
    List<ResultSet> resultSets = new ArrayList<>();
    for (ResultSetGroup resultSetGroup : resultSetGroups) {
      for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
        resultSets.add(resultSetGroup.getResultSet(i));
      }
    }
    return resultSets;
  }

  private static String format(ResultSetGroup resultSetGroup) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
      sb.append(resultSetGroup.getResultSet(i));
      sb.append("\n");
    }
    return sb.toString();
  }

  private List<String[]> parseResultSets(ThirdEyeRequest request, List<ResultSet> resultSets,
      List<MetricFunction> metricFunctions, CollectionSchema collectionSchema,
      List<String> dimensionNames) {

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
    TimeUnit dataTimeUnit = null;
    long startTime = request.getStartTimeInclusive().getMillis();
    long interval = -1;
    dataTimeUnit = collectionSchema.getTime().getDataGranularity().getUnit();
    boolean isISOFormat = false;
    DateTimeFormatter dateTimeFormatter = null;
    String timeFormat = collectionSchema.getTime().getFormat();
    if (timeFormat != null && !timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
      isISOFormat = true;
      dateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZoneUTC();
    }
    if (request.getGroupByTimeGranularity() != null) {
      hasGroupByTime = true;
      interval = request.getGroupByTimeGranularity().toMillis();
    }
    LinkedHashMap<String, String[]> dataMap = new LinkedHashMap<>();
    for (int i = 0; i < resultSets.size(); i++) {
      ResultSet resultSet = resultSets.get(i);
      int numRows = resultSet.getRowCount();
      for (int r = 0; r < numRows; r++) {
        boolean skipRowDueToError = false;
        String[] groupKeys;
        if (hasGroupBy) {
          groupKeys = new String[resultSet.getGroupKeyLength()];
          for (int grpKeyIdx = 0; grpKeyIdx < resultSet.getGroupKeyLength(); grpKeyIdx++) {
            String groupKeyVal = resultSet.getGroupKeyString(r, grpKeyIdx);
            if (hasGroupByTime && grpKeyIdx == 0) {
              int timeBucket;
              long millis;
              if (!isISOFormat) {
                millis = dataTimeUnit.toMillis(Double.valueOf(groupKeyVal).longValue());
              } else {
                millis = DateTime.parse(groupKeyVal, dateTimeFormatter).getMillis();
              }
              if (millis < startTime) {
                LOG.error("Data point earlier than requested start time {}: {}", startTime, millis);
                skipRowDueToError = true;
                break;
              }
              timeBucket = (int) ((millis - startTime) / interval);
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
        rowValues[groupKeys.length + i] =
            String.valueOf(Double.parseDouble(rowValues[groupKeys.length + i])
                + Double.parseDouble(resultSet.getString(r, 0)));
      }
    }
    List<String[]> rows = new ArrayList<>();
    rows.addAll(dataMap.values());
    return rows;

  }

  @Override
  public CollectionSchema getCollectionSchema(String collection) throws Exception {
    return CACHE_INSTANCE.getCollectionSchemaCache().get(collection);
  }

  @Override
  public List<String> getCollections() throws Exception {
    return CACHE_INSTANCE.getCollectionsCache().getCollections();
  }

  @Override
  public CollectionConfig getCollectionConfig(String collection) throws Exception {
    return CACHE_INSTANCE.getCollectionConfigCache().get(collection);
  }

  @Override
  public long getMaxDataTime(String collection) throws Exception {
    return CACHE_INSTANCE.getCollectionMaxDataTimeCache().get(collection);
  }

  @Override
  public void clear() throws Exception {
  }

  @Override
  public void close() throws Exception {
    controllerClient.close();
  }

  /** TESTING ONLY - WE SHOULD NOT BE USING THIS. */
  @Deprecated
  public static PinotThirdEyeClient getDefaultTestClient() {
    // TODO REPLACE WITH CONFIGS
    String controllerHost = "localhost";
    int controllerPort = 11984;
    String zkUrl =
        "localhost:12913/pinot-cluster";
    String clusterName = "mpSprintDemoCluster";
    String tag = "thirdeye_BROKER";
    // return fromZookeeper(controllerHost, controllerPort, zkUrl, clusterName, tag);
    return fromHostList(controllerHost, controllerPort, "localhost:7001");
  }
}
