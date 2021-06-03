package org.apache.pinot.integration.tests;

import cloud.localstack.Localstack;
import cloud.localstack.docker.annotation.LocalstackDockerAnnotationProcessor;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import cloud.localstack.docker.command.Command;
import cloud.localstack.docker.exception.LocalstackDockerException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.plugin.stream.kinesis.KinesisConfig;
import org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.utils.AttributeMap;

import static org.awaitility.Awaitility.await;


@LocalstackDockerProperties(services = {"kinesis", "dynamodb"})
public class RealtimeKinesisIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final LocalstackDockerAnnotationProcessor PROCESSOR = new LocalstackDockerAnnotationProcessor();
  private static final String STREAM_NAME = "kinesis-test";
  private static final String STREAM_TYPE = "kinesis";
  public static final int MAX_RECORDS_TO_FETCH = 2000;

  public static final String REGION = "us-east-1";
  public static final String LOCALSTACK_KINESIS_ENDPOINT = "http://localhost:4566";
  public static final int NUM_SHARDS = 10;

  public static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  public static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";

  private final Localstack localstackDocker = Localstack.INSTANCE;

  private static KinesisClient kinesisClient = null;

  private long totalRecordsPushedInStream = 0;

  List<String> h2FieldNameAndTypes = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kinesis
    startKinesis();

    // Create and upload the schema and table config
    addSchema(createKinesisSchema());
    addTableConfig(createKinesisTableConfig());

    createH2ConnectionAndTable();

    // Push data into Kinesis
    publishRecordsToKinesis();

    // Wait for all documents loaded
    waitForAllDocsLoadedKinesis(60_000L);
  }

  public Schema createKinesisSchema()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(SCHEMA_FILE_PATH);
    Assert.assertNotNull(resourceUrl);
    return Schema.fromFile(new File(resourceUrl.getFile()));
  }

  protected void waitForAllDocsLoadedKinesis(long timeoutMs)
      throws Exception {
    waitForAllDocsLoadedKinesis(timeoutMs, true);
  }

  protected void waitForAllDocsLoadedKinesis(long timeoutMs, boolean raiseError) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() >= totalRecordsPushedInStream;
        } catch (Exception e) {
          return null;
        }
      }
    }, 1000L, timeoutMs, "Failed to load " + totalRecordsPushedInStream + " documents", raiseError);
  }

  public TableConfig createKinesisTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getTableName())
        .setTimeColumnName("DaysSinceEpoch").setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setLLC(true).setStreamConfigs(createKinesisStreamConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  public Map<String, String> createKinesisStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kinesis";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);

    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
            STREAM_NAME);

    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "30000");
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            StreamConfig.ConsumerType.LOWLEVEL.toString());
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        KinesisConsumerFactory.class.getName());
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
            "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    streamConfigMap.put(KinesisConfig.REGION, REGION);
    streamConfigMap.put(KinesisConfig.MAX_RECORDS_TO_FETCH, String.valueOf(MAX_RECORDS_TO_FETCH));
    streamConfigMap.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    streamConfigMap.put(KinesisConfig.ENDPOINT, LOCALSTACK_KINESIS_ENDPOINT);
    streamConfigMap
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(getRealtimeSegmentFlushSize()));
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  public void startKinesis()
      throws Exception {
    try {
      final LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
      localstackDocker.startup(dockerConfig);
    } catch (LocalstackDockerException e) {
      StopAllLocalstackDockerCommand stopAllLocalstackDockerCommand = new StopAllLocalstackDockerCommand();
      stopAllLocalstackDockerCommand.execute();

      final LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
      localstackDocker.startup(dockerConfig);
    }

    kinesisClient = KinesisClient.builder().httpClient(new ApacheSdkHttpService().createHttpClientBuilder()
        .buildWithDefaults(
            AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .credentialsProvider(getLocalAWSCredentials()).region(Region.of(REGION))
        .endpointOverride(new URI(LOCALSTACK_KINESIS_ENDPOINT)).build();

    kinesisClient.createStream(CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(NUM_SHARDS).build());
    await().until(() -> kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(STREAM_NAME).build())
        .streamDescription().streamStatusAsString().equals("ACTIVE"));
  }

  public void stopKinesis() {
    if (localstackDocker.isRunning()) {
      localstackDocker.stop();
    }
  }

  private void publishRecordsToKinesis() {
    try {
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < h2FieldNameAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + getTableName() + " VALUES (" + params.toString() + ")");

      InputStream inputStream =
          RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          JsonNode data = JsonUtils.stringToJsonNode(line);

          PutRecordRequest putRecordRequest =
              PutRecordRequest.builder().streamName(STREAM_NAME).data(SdkBytes.fromUtf8String(line))
                  .partitionKey(data.get("Origin").textValue()).build();
          PutRecordResponse putRecordResponse = kinesisClient.putRecord(putRecordRequest);
          if (putRecordResponse.sdkHttpResponse().statusCode() == 200) {
            if (StringUtils.isNotBlank(putRecordResponse.sequenceNumber()) && StringUtils
                .isNotBlank(putRecordResponse.shardId())) {
              totalRecordsPushedInStream++;
              h2Statement.setObject(1, data.get("Quarter").intValue());
              h2Statement.setObject(2, data.get("FlightNum").intValue());
              h2Statement.setObject(3, data.get("Origin").textValue());
              h2Statement.setObject(4, data.get("Destination").textValue());
              h2Statement.setObject(5, data.get("DaysSinceEpoch").intValue());

              h2Statement.execute();
            }
          }
        }
      }

      inputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Could not publish records to Kinesis Stream", e);
    }
  }

  private static AwsCredentialsProvider getLocalAWSCredentials()
      throws Exception {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
  }

  @Test
  public void testRecords()
      throws Exception {
    Assert.assertNotEquals(totalRecordsPushedInStream, 0);

    ResultSet pinotResultSet = getPinotConnection()
        .execute(new Request("sql", "SELECT * FROM " + getTableName() + " ORDER BY Origin LIMIT 10000"))
        .getResultSet(0);

    Assert.assertNotEquals(pinotResultSet.getRowCount(), 0);

    Statement h2statement =
        _h2Connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
    h2statement.execute("SELECT * FROM " + getTableName() + " ORDER BY Origin");
    java.sql.ResultSet h2ResultSet = h2statement.getResultSet();

    Assert.assertFalse(h2ResultSet.isLast());

    h2ResultSet.beforeFirst();
    int row = 0;
    Map<String, Integer> columnToIndex = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      columnToIndex.put(pinotResultSet.getColumnName(i), i);
    }

    while (h2ResultSet.next()) {
      int expectedQuarter = h2ResultSet.getInt("Quarter");
      int expectedFlightNum = h2ResultSet.getInt("FlightNum");
      String expectedOrigin = h2ResultSet.getString("Origin");
      String expectedDestination = h2ResultSet.getString("Destination");

      int actualQuarter = pinotResultSet.getInt(row, columnToIndex.get("Quarter"));
      int actualFlightNum = pinotResultSet.getInt(row, columnToIndex.get("FlightNum"));
      String actualOrigin = pinotResultSet.getString(row, columnToIndex.get("Origin"));
      String actualDestination = pinotResultSet.getString(row, columnToIndex.get("Destination"));

      Assert.assertEquals(actualQuarter, expectedQuarter);
      Assert.assertEquals(actualFlightNum, expectedFlightNum);
      Assert.assertEquals(actualOrigin, expectedOrigin);
      Assert.assertEquals(actualDestination, expectedDestination);

      row++;

      if (row >= pinotResultSet.getRowCount()) {
        int cnt = 0;
        while (h2ResultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(cnt, 0);
        break;
      }
    }
  }

  @Test
  public void testCountRecords() {
    long count =
        getPinotConnection().execute(new Request("sql", "SELECT COUNT(*) FROM " + getTableName())).getResultSet(0)
            .getLong(0);

    Assert.assertEquals(count, totalRecordsPushedInStream);
  }

  public void createH2ConnectionAndTable()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
    _h2Connection.prepareCall("DROP TABLE IF EXISTS " + getTableName()).execute();

    h2FieldNameAndTypes = new ArrayList<>();

    h2FieldNameAndTypes.add("Quarter bigint");
    h2FieldNameAndTypes.add("FlightNum bigint");
    h2FieldNameAndTypes.add("Origin varchar(128)");
    h2FieldNameAndTypes.add("Destination varchar(128)");
    h2FieldNameAndTypes.add("DaysSinceEpoch bigint");

    _h2Connection.prepareCall("CREATE TABLE " + getTableName() + "(" + StringUtil
        .join(",", h2FieldNameAndTypes.toArray(new String[h2FieldNameAndTypes.size()])) + ")").execute();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    stopKinesis();
    FileUtils.deleteDirectory(_tempDir);
  }

  public static class StopAllLocalstackDockerCommand extends Command {

    public void execute() {
      String runningDockerContainers =
          dockerExe.execute(Arrays.asList("ps", "-a", "-q", "-f", "ancestor=localstack/localstack"));
      if (StringUtils.isNotBlank(runningDockerContainers) && !runningDockerContainers.toLowerCase().contains("error")) {
        String[] containerList = runningDockerContainers.split("\n");

        for (String containerId : containerList) {
          dockerExe.execute(Arrays.asList("stop", containerId));
        }
      }
    }
  }
}
