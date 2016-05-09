package com.linkedin.thirdeye.client.pinot;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.util.HelixUtil;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.PinotClientException;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;

/**
 * ThirdEyeClient that uses {@link Connection} to query data, and the Pinot Controller REST
 * endpoints for querying tables and schemas. Because of the controller dependency, schemas must be
 * provided to the cluster controller even if all cluster data is offline. <br/>
 * While this class does provide some caching on PQL queries, it is recommended to use
 * {@link CachedThirdEyeClient} or instantiate this class via {@link PinotThirdEyeClientFactory}
 * to improve performance. Instances of this class can be created from the static factory methods
 * (from*).
 * @author jteoh
 */
public class PinotThirdEyeClient implements ThirdEyeClient {
  public static final String CONTROLLER_HOST_PROPERTY_KEY = "controllerHost";
  public static final String CONTROLLER_PORT_PROPERTY_KEY = "controllerPort";
  public static final String FIXED_COLLECTIONS_PROPERTY_KEY = "fixedCollections";
  public static final String CLUSTER_NAME_PROPERTY_KEY = "clusterName";
  public static final String TAG_PROPERTY_KEY = "tag";

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeClient.class);

  private static final String UTF_8 = "UTF-8";
  private static final String TABLES_ENDPOINT = "tables/";
  private static final String BROKER_PREFIX = "Broker_";

  // No way to determine data retention from pinot schema
  private static final TimeGranularity DEFAULT_TIME_RETENTION = null;
  // Pinot TimeFieldSpec always assumes granularity size of 1.
  private static final int GRANULARITY_SIZE = 1;

  private final Connection connection;
  private final LoadingCache<String, ResultSetGroup> resultSetGroupCache;
  private final LoadingCache<String, Schema> schemaCache;
  private final LoadingCache<String, CollectionSchema> collectionSchemaCache;
  private final LoadingCache<String, Long> collectionMaxDataTimeCache;
  String segementZKMetadataRootPath;
  private final HttpHost controllerHost;
  private final CloseableHttpClient controllerClient;
  private List<String> fixedCollections = null;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;

  protected PinotThirdEyeClient(Connection connection, String controllerHostName,
      int controllerPort, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this.connection = connection;
    this.propertyStore = propertyStore;
    // TODO make this more configurable? (leverage cache config)
    this.resultSetGroupCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES)
        .build(new ResultSetGroupCacheLoader());
    this.schemaCache = CacheBuilder.newBuilder().build(new SchemaCacheLoader());
    this.collectionSchemaCache = CacheBuilder.newBuilder().build(new CollectionSchemaCacheLoader());
    this.collectionMaxDataTimeCache = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES).build(new CollectionMaxDataTimeCacheLoader());

    this.controllerHost = new HttpHost(controllerHostName, controllerPort);
    // TODO currently no way to configure the CloseableHttpClient
    this.controllerClient = HttpClients.createDefault();
    LOG.info("Created PinotThirdEyeClient to {} with controller {}", connection, controllerHost);
  }

  /* Static builder methods to mirror Pinot Java API (ConnectionFactory) */
  public static PinotThirdEyeClient fromHostList(String controllerHost, int controllerPort,
      String... brokers) {
    if (brokers == null || brokers.length == 0) {
      throw new IllegalArgumentException("Please specify at least one broker.");
    }
    Connection connection = ConnectionFactory.fromHostList(brokers);
    LOG.info("Created PinotThirdEyeClient to hosts: {}", (Object[]) brokers);
    return new PinotThirdEyeClient(connection, controllerHost, controllerPort, null);
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
      String zkUrl, String clusterName, String tag) {
    ZkClient zkClient = new ZkClient(zkUrl);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected();
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
    List<String> thirdeyeBrokerList = helixAdmin.getInstancesInClusterWithTag(clusterName, tag);
    LOG.info("Found brokers:{} with tag:{}", thirdeyeBrokerList, tag);
    if (thirdeyeBrokerList.size() == 0) {
      throw new RuntimeException("No brokers available with tag:" + tag);
    }
    String[] thirdeyeBrokers = new String[thirdeyeBrokerList.size()];
    for (int i = 0; i < thirdeyeBrokerList.size(); i++) {
      String instanceName = thirdeyeBrokerList.get(i);
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
      thirdeyeBrokers[i] = instanceConfig.getHostName().replaceAll(BROKER_PREFIX, "") + ":"
          + instanceConfig.getPort();
    }
    Connection connection = ConnectionFactory.fromHostList(thirdeyeBrokers);
    LOG.info("Created PinotThirdEyeClient to zookeeper: {}", zkUrl);
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(zkClient);
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);

    List<String> watchList = null;
    ZkHelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(baseAccessor, propertyStorePath, watchList);
    return new PinotThirdEyeClient(connection, controllerHost, controllerPort, propertyStore);
  }

  public static PinotThirdEyeClient fromProperties(Properties properties) {
    Connection connection = ConnectionFactory.fromProperties(properties);
    LOG.info("Created PinotThirdEyeClient from properties {}", properties);
    if (!properties.containsKey(CONTROLLER_HOST_PROPERTY_KEY)
        || !properties.containsKey(CONTROLLER_PORT_PROPERTY_KEY)) {
      throw new IllegalArgumentException("Properties file must contain controller mappings for "
          + CONTROLLER_HOST_PROPERTY_KEY + " and " + CONTROLLER_PORT_PROPERTY_KEY);
    }
    return new PinotThirdEyeClient(connection, properties.getProperty(CONTROLLER_HOST_PROPERTY_KEY),
        Integer.valueOf(properties.getProperty(CONTROLLER_PORT_PROPERTY_KEY)), null);
  }

  public static ThirdEyeClient fromClientConfig(PinotThirdEyeClientConfig config) {
    return fromZookeeper(config.getControllerHost(), config.getControllerPort(),
        config.getZookeeperUrl(), config.getClusterName(), config.getTag());
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    CollectionSchema collectionSchema = collectionSchemaCache.get(request.getCollection());
    TimeSpec dataTimeSpec = collectionSchema.getTime();
    List<MetricFunction> metricFunctions = request.getMetricFunctions();
    List<String> dimensionNames = collectionSchema.getDimensionNames();
    String sql = PqlUtils.getPql(request, dataTimeSpec);
    LOG.debug("Executing: {}", sql);
    ResultSetGroup result = resultSetGroupCache.get(sql);
    parseResultSetGroup(request, result, metricFunctions, collectionSchema, dimensionNames);
    ThirdEyeResponse resp = new ThirdEyeResponse(request, result, dataTimeSpec);
    return resp;
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
    Schema schema = getSchema(collection);
    List<DimensionSpec> dimSpecs = fromDimensionFieldSpecs(schema.getDimensionFieldSpecs());
    List<MetricSpec> metricSpecs = fromMetricFieldSpecs(schema.getMetricFieldSpecs());
    TimeSpec timeSpec = fromTimeFieldSpec(schema.getTimeFieldSpec());
    CollectionSchema config = new CollectionSchema.Builder().setCollection(collection)
        .setDimensions(dimSpecs).setMetrics(metricSpecs).setTime(timeSpec).build();
    return config;
  }

  /**
   * Hardcodes a set of collections. Note that this method assumes the schemas for
   * these collections already exist.
   */
  public void setFixedCollections(List<String> collections) {
    LOG.info("Setting fixed collections: {}", collections);
    this.fixedCollections = collections;
  }

  @Override
  public List<String> getCollections() throws Exception {
    if (this.fixedCollections != null) {
      // assume the fixed collections are correct.
      return fixedCollections;
    }
    HttpGet req = new HttpGet(TABLES_ENDPOINT);
    LOG.info("Retrieving collections: {}", req);
    CloseableHttpResponse res = controllerClient.execute(controllerHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      JsonNode tables = new ObjectMapper().readTree(content).get("tables");
      ArrayList<String> collections = new ArrayList<>(tables.size());
      ArrayList<String> skippedCollections = new ArrayList<>();
      for (JsonNode table : tables) {
        String collection = table.asText();
        if (!collection.startsWith("thirdeye")) {
          continue;
        }
        // TODO Since Pinot does not strictly require a schema to be provided for each offline data
        // set, filter out those for which a schema cannot be retrieved.
        try {
          Schema schema = getSchema(collection);
          if (schema == null) {
            LOG.debug("Skipping collection {} due to null schema", collection);
            skippedCollections.add(collection);
            continue;
          }
        } catch (Exception e) {
          LOG.debug("Skipping collection {} due to schema retrieval exception", collection, e);
          skippedCollections.add(collection);
          continue;
        }
        collections.add(collection);
      }
      if (!skippedCollections.isEmpty()) {
        LOG.info(
            "{} collections were not included because their schemas could not be retrieved: {}",
            skippedCollections.size(), skippedCollections);
      }

      return collections;
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }

  @Override
  public void clear() throws Exception {
    resultSetGroupCache.invalidateAll();
    schemaCache.invalidateAll();
  }

  @Override
  public void close() throws Exception {
    controllerClient.close();
  }

  private Schema getSchema(String collection) throws ClientProtocolException, IOException,
      InterruptedException, ExecutionException, TimeoutException {
    return schemaCache.get(collection);
  }

  private List<DimensionSpec> fromDimensionFieldSpecs(List<DimensionFieldSpec> specs) {
    List<DimensionSpec> results = new ArrayList<>(specs.size());
    for (DimensionFieldSpec dimensionFieldSpec : specs) {
      DimensionSpec dimensionSpec = new DimensionSpec(dimensionFieldSpec.getName());
      results.add(dimensionSpec);
    }
    return results;
  }

  private List<MetricSpec> fromMetricFieldSpecs(List<MetricFieldSpec> specs) {
    ArrayList<MetricSpec> results = new ArrayList<>(specs.size());
    for (MetricFieldSpec metricFieldSpec : specs) {
      MetricSpec metricSpec = getMetricType(metricFieldSpec);
      results.add(metricSpec);
    }
    return results;
  }

  private MetricSpec getMetricType(MetricFieldSpec metricFieldSpec) {
    DataType dataType = metricFieldSpec.getDataType();
    MetricType metricType;
    switch (dataType) {
    case BOOLEAN:
    case BYTE:
    case BYTE_ARRAY:
    case CHAR:
    case CHAR_ARRAY:
    case DOUBLE_ARRAY:
    case FLOAT_ARRAY:
    case INT_ARRAY:
    case LONG_ARRAY:
    case OBJECT:
    case SHORT_ARRAY:
    case STRING:
    case STRING_ARRAY:
    default:
      throw new UnsupportedOperationException(dataType + " is not a supported metric type");
    case DOUBLE:
      metricType = MetricType.DOUBLE;
      break;
    case FLOAT:
      metricType = MetricType.FLOAT;
      break;
    case INT:
      metricType = MetricType.INT;
      break;
    case LONG:
      metricType = MetricType.LONG;
      break;
    case SHORT:
      metricType = MetricType.SHORT;
      break;

    }
    MetricSpec metricSpec = new MetricSpec(metricFieldSpec.getName(), metricType);
    return metricSpec;
  }

  private TimeSpec fromTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    TimeGranularity inputGranularity = new TimeGranularity(GRANULARITY_SIZE,
        timeFieldSpec.getIncomingGranularitySpec().getTimeType());
    TimeGranularity outputGranularity = new TimeGranularity(GRANULARITY_SIZE,
        timeFieldSpec.getOutgoingGranularitySpec().getTimeType());
    TimeSpec spec = new TimeSpec(timeFieldSpec.getOutGoingTimeColumnName(), inputGranularity,
        outputGranularity, DEFAULT_TIME_RETENTION);
    return spec;
  }

  private class ResultSetGroupCacheLoader extends CacheLoader<String, ResultSetGroup> {
    @Override
    public ResultSetGroup load(String sql) throws Exception {
      try {
        ResultSetGroup resultSetGroup = connection.execute(sql);
        return resultSetGroup;
      } catch (PinotClientException cause) {
        throw new PinotClientException("Error when running sql:" + sql, cause);
      }
    }
  }

  private class SchemaCacheLoader extends CacheLoader<String, Schema> {
    @Override
    public Schema load(String collection) throws Exception {
      HttpGet req = new HttpGet(TABLES_ENDPOINT + URLEncoder.encode(collection, UTF_8) + "/schema");
      LOG.info("Retrieving schema: {}", req);
      CloseableHttpResponse res = controllerClient.execute(controllerHost, req);
      try {
        if (res.getStatusLine().getStatusCode() != 200) {
          throw new IllegalStateException(res.getStatusLine().toString());
        }
        InputStream content = res.getEntity().getContent();
        Schema schema = new ObjectMapper().readValue(content, Schema.class);
        return schema;
      } finally {
        if (res.getEntity() != null) {
          EntityUtils.consume(res.getEntity());
        }
        res.close();
      }
    }
  }

  private class CollectionSchemaCacheLoader extends CacheLoader<String, CollectionSchema> {
    @Override
    public CollectionSchema load(String collection) throws Exception {
      return getCollectionSchema(collection);
    }
  }

  public class CollectionMaxDataTimeCacheLoader extends CacheLoader<String, Long> {

    @Override
    public Long load(String collection) throws Exception {
      long maxTime = 0;
      if (propertyStore != null) {
        List<Stat> stats = new ArrayList<>();
        int options = AccessOption.PERSISTENT;
        List<ZNRecord> zkSegmentDataList =
            propertyStore.getChildren("/SEGMENTS/" + collection + "_OFFLINE", stats, options);
        for (ZNRecord record : zkSegmentDataList) {
          try {
            long endTime = Long.parseLong(record.getSimpleField("segment.end.time"));
            TimeUnit timeUnit = TimeUnit.valueOf(record.getSimpleField("segment.time.unit"));
            long endTimeMillis = timeUnit.toMillis(endTime);
            if (endTimeMillis > maxTime) {
              maxTime = endTimeMillis;
            }
          } catch (Exception e) {
            LOG.warn("Exception parsing metadata:{}", record, e);
          }
        }
      } else {
        maxTime = System.currentTimeMillis();
      }
      return maxTime;
    }

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
    return fromZookeeper(controllerHost, controllerPort, zkUrl, clusterName, tag);

    // return fromHostList(controllerHost, controllerPort,
    // "lva1-app0437.corp.linkedin.com:7001");
  }

  public static void main(String[] args) throws Exception {
    PinotThirdEyeClient pinotThirdEyeClient = PinotThirdEyeClient.getDefaultTestClient(); // TODO
                                                                                          // make
                                                                                          // this
    // configurable
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection("thirdeyeAbook_OFFLINE");
    builder.setStartTimeInclusive(new DateTime(2016, 1, 1, 00, 00));
    builder.setEndTimeExclusive(new DateTime(2016, 1, 2, 00, 00));
    builder.setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS));
    builder.setGroupBy("browserName");
    List<String> metrics =
        Lists.newArrayList("__COUNT", "totalFlows", "suggestedMemberInvitations");
    for (String metric : metrics) {
      MetricFunction metricFunction = new MetricFunction(MetricFunction.SUM, metric);
      builder.addMetricFunction(metricFunction);
    }
    ThirdEyeResponse result = pinotThirdEyeClient.execute(builder.build("test"));
    // Number[] metricValues = new Number[metrics.size()];
    // for (DimensionKey key : result.keySet()) {
    // MetricTimeSeries metricTimeSeries = result.get(key);
    // for (long timeWindow : metricTimeSeries.getTimeWindowSet()) {
    // StringBuilder sb = new StringBuilder(key.toString());
    // sb.append(timeWindow);
    // for (int i = 0; i < metrics.size(); i++) {
    // String metricName = metrics.get(i);
    // metricValues[i] = metricTimeSeries.get(timeWindow, metricName);
    // }
    // sb.append(Arrays.toString(metricValues));
    // System.out.println(sb);
    // }
    // }
    System.exit(1);
  }

  @Override
  public long getMaxDataTime(String collection) throws Exception {
    return collectionMaxDataTimeCache.get(collection);
  }

}
