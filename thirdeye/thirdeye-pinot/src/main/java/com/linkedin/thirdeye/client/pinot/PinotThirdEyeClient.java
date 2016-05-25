package com.linkedin.thirdeye.client.pinot;

import java.util.List;
import java.util.Properties;

import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

public class PinotThirdEyeClient implements ThirdEyeClient {
  private static final ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  public static final String CONTROLLER_HOST_PROPERTY_KEY = "controllerHost";
  public static final String CONTROLLER_PORT_PROPERTY_KEY = "controllerPort";
  public static final String FIXED_COLLECTIONS_PROPERTY_KEY = "fixedCollections";
  public static final String CLUSTER_NAME_PROPERTY_KEY = "clusterName";
  public static final String TAG_PROPERTY_KEY = "tag";
  private static final String BROKER_PREFIX = "Broker_";

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeClient.class);

  String segementZKMetadataRootPath;
  private final HttpHost controllerHost;
  private final CloseableHttpClient controllerClient;

  protected PinotThirdEyeClient(String controllerHostName, int controllerPort) {

    this.controllerHost = new HttpHost(controllerHostName, controllerPort);
    // TODO currently no way to configure the CloseableHttpClient
    this.controllerClient = HttpClients.createDefault();
    LOG.info("Created PinotThirdEyeClient to {} with controller {}", controllerHost);
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
    String sql = PqlUtils.getPql(request, dataTimeSpec);
    LOG.info("Executing: {}", sql);
    ResultSetGroup result = CACHE_INSTANCE.getResultSetGroupCache()
        .get(new PinotQuery(sql, request.getCollection() + "_OFFLINE"));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Result for: {} {}", sql, format(result));
    }
    parseResultSetGroup(request, result, metricFunctions, collectionSchema, dimensionNames);
    PinotThirdEyeResponse resp = new PinotThirdEyeResponse(request, result, dataTimeSpec);
    return resp;
  }

  private static String format(ResultSetGroup result) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < result.getResultSetCount(); i++) {
      ResultSet resultSet = result.getResultSet(i);
      for (int c = 0; c < resultSet.getColumnCount(); c++) {
        sb.append(resultSet.getColumnName(c)).append("=").append(resultSet.getDouble(c));
      }
    }
    return sb.toString();
  }

  private void parseResultSetGroup(ThirdEyeRequest request, ResultSetGroup result,
      List<MetricFunction> metricFunctions, CollectionSchema collectionSchema,
      List<String> dimensionNames)
      throws JsonProcessingException, RuntimeException, NumberFormatException {

    for (int groupIdx = 0; groupIdx < result.getResultSetCount(); groupIdx++) {
      ResultSet resultSet = result.getResultSet(groupIdx);
      for (int row = 0; row < resultSet.getRowCount(); row++) {
        StringBuilder sb = new StringBuilder();
        String delim = "";
        if (resultSet.getGroupKeyLength() > 0) {
          for (int groupKey = 0; groupKey < resultSet.getGroupKeyLength(); groupKey++) {
            sb.append(delim).append(resultSet.getGroupKeyString(row, groupKey));
            delim = ",";
          }
        }
        int columnCount = resultSet.getColumnCount();
        for (int col = 0; col < columnCount; col++) {
          String colValStr = resultSet.getString(row, col);
          sb.append(delim).append(colValStr);
          delim = ",";
        }
        // LOG.debug("{}", sb);
      }
    }
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
    String controllerHost = "lva1-app0086.corp.linkedin.com";
    int controllerPort = 11984;
    String zkUrl = "zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster";// "zk-lva1-pinot.corp:12913/pinot-cluster";
    String clusterName = "mpSprintDemoCluster";
    String tag = "thirdeye_BROKER";
    // return fromZookeeper(controllerHost, controllerPort, zkUrl, clusterName, tag);

    return fromHostList(controllerHost, controllerPort, "lva1-app0091.corp.linkedin.com:7001");
  }

  public static void main(String[] args) throws Exception {
    String controllerHost = "lva1-app0086.corp.linkedin.com";
    int controllerPort = 11984;
    String zkUrl = "zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster";// "zk-lva1-pinot.corp:12913/pinot-cluster";
    String clusterName = "mpSprintDemoCluster";

    // RAW connection
    Connection connection = ConnectionFactory.fromZookeeper(zkUrl + "/" + clusterName);
    String statement =
        "SELECT sum(engaged_feed_session_count) FROM feed_sessions_additive WHERE  Date = 20160514";
    ResultSetGroup resultSetGroup = connection.execute("feed_sessions_additive_OFFLINE", statement);
    System.out.println(format(resultSetGroup));

    // thirdeyeClient
    ResultSetGroupCacheLoader resultSetGroupCacheLoader = new ResultSetGroupCacheLoader(connection);
    // ThirdeyeCacheRegistry.getInstance().registerResultSetGroupCache(resultSetGroupCacheLoader);

    PinotThirdEyeClient thirdEyeClient =
        PinotThirdEyeClient.fromZookeeper(controllerHost, controllerPort, zkUrl, clusterName);
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection("feed_sessions_additive");
    builder.setStartTimeInclusive(DateTime.parse("2016-05-11"));
    builder.setEndTimeExclusive(DateTime.parse("2016-05-11"));
    builder.setMetricFunctions(
        Lists.newArrayList(new MetricFunction("SUM", "sum_engaged_feed_session_count")));
    ThirdEyeRequest thirdEyeRequest = builder.build("asd");
    // ThirdEyeResponse response = thirdEyeClient.execute(thirdEyeRequest);
    // System.out.println("Response: " + response);

  }

  @Override
  public long getMaxDataTime(String collection) throws Exception {
    return CACHE_INSTANCE.getCollectionMaxDataTimeCache().get(collection);
  }

}
