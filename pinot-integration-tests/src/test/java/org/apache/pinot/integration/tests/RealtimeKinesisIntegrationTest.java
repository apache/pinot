/**
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
package org.apache.pinot.integration.tests;

import cloud.localstack.Localstack;
import cloud.localstack.ServiceName;
import cloud.localstack.docker.annotation.LocalstackDockerAnnotationProcessor;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import cloud.localstack.docker.command.Command;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.activation.UnsupportedDataTypeException;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.plugin.stream.kinesis.KinesisConfig;
import org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
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


@LocalstackDockerProperties(services = {ServiceName.KINESIS}, imageTag = "0.12.15")
public class RealtimeKinesisIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeKinesisIntegrationTest.class);

  private static final LocalstackDockerAnnotationProcessor PROCESSOR = new LocalstackDockerAnnotationProcessor();
  private static final String STREAM_NAME = "kinesis-test";
  private static final String STREAM_TYPE = "kinesis";
  public static final int MAX_RECORDS_TO_FETCH = Integer.MAX_VALUE;

  public static final String REGION = "us-east-1";
  public static final String LOCALSTACK_KINESIS_ENDPOINT = "http://localhost:4566";
  public static final int NUM_SHARDS = 10;

  // Localstack Kinesis doesn't support large rows.
  // So, this airlineStats data file consists of only few fields and rows from the original data
  public static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  public static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";

  private final Localstack _localstackDocker = Localstack.INSTANCE;

  private static KinesisClient _kinesisClient = null;

  private long _totalRecordsPushedInStream = 0;

  List<String> _h2FieldNameAndTypes = new ArrayList<>();

  private boolean _skipTestNoDockerInstalled = false;

  @BeforeClass(enabled = false)
  public void setUp()
      throws Exception {
    try {
      DockerInfoCommand dockerInfoCommand = new DockerInfoCommand();
      dockerInfoCommand.execute();
    } catch (IllegalStateException e) {
      _skipTestNoDockerInstalled = true;
      LOGGER.warn("Skipping test! Docker is not found running", e);
      throw new SkipException(e.getMessage());
    }

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
    waitForAllDocsLoadedKinesis(120_000L);
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
          return getCurrentCountStarResult() >= _totalRecordsPushedInStream;
        } catch (Exception e) {
          LOGGER.warn("Could not fetch current number of rows in pinot table " + getTableName(), e);
          return null;
        }
      }
    }, 1000L, timeoutMs, "Failed to load " + _totalRecordsPushedInStream + " documents", raiseError);
  }

  public TableConfig createKinesisTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getTableName())
        .setTimeColumnName("DaysSinceEpoch").setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setStreamConfigs(createKinesisStreamConfig()).setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  public Map<String, String> createKinesisStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kinesis";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);

    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        STREAM_NAME);

    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "30000");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), KinesisConsumerFactory.class.getName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    streamConfigMap.put(KinesisConfig.REGION, REGION);
    streamConfigMap.put(KinesisConfig.MAX_RECORDS_TO_FETCH, String.valueOf(MAX_RECORDS_TO_FETCH));
    streamConfigMap.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
    streamConfigMap.put(KinesisConfig.ENDPOINT, LOCALSTACK_KINESIS_ENDPOINT);
    streamConfigMap.put(KinesisConfig.ACCESS_KEY, getLocalAWSCredentials().resolveCredentials().accessKeyId());
    streamConfigMap.put(KinesisConfig.SECRET_KEY, getLocalAWSCredentials().resolveCredentials().secretAccessKey());
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(200));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  public void startKinesis()
      throws Exception {
    final LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
    StopAllLocalstackDockerCommand stopAllLocalstackDockerCommand = new StopAllLocalstackDockerCommand();
    stopAllLocalstackDockerCommand.execute();
    _localstackDocker.startup(dockerConfig);

    _kinesisClient = KinesisClient.builder().httpClient(new ApacheSdkHttpService().createHttpClientBuilder()
            .buildWithDefaults(
                AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .credentialsProvider(getLocalAWSCredentials()).region(Region.of(REGION))
        .endpointOverride(new URI(LOCALSTACK_KINESIS_ENDPOINT)).build();

    _kinesisClient.createStream(CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(NUM_SHARDS).build());

    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          String kinesisStreamStatus =
              _kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(STREAM_NAME).build())
                  .streamDescription().streamStatusAsString();

          return kinesisStreamStatus.contentEquals("ACTIVE");
        } catch (Exception e) {
          LOGGER.warn("Could not fetch kinesis stream status", e);
          return null;
        }
      }
    }, 1000L, 30000, "Kinesis stream " + STREAM_NAME + " is not created or is not in active state", true);
  }

  public void stopKinesis() {
    if (_localstackDocker.isRunning()) {
      _localstackDocker.stop();
    }
  }

  private void publishRecordsToKinesis() {
    try {
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < _h2FieldNameAndTypes.size() - 1; i++) {
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
          PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequest);
          if (putRecordResponse.sdkHttpResponse().statusCode() == 200) {
            if (StringUtils.isNotBlank(putRecordResponse.sequenceNumber()) && StringUtils.isNotBlank(
                putRecordResponse.shardId())) {
              _totalRecordsPushedInStream++;

              int fieldIndex = 1;
              for (String fieldNameAndDatatype : _h2FieldNameAndTypes) {
                String[] fieldNameAndDatatypeList = fieldNameAndDatatype.split(" ");
                String fieldName = fieldNameAndDatatypeList[0];
                String h2DataType = fieldNameAndDatatypeList[1];
                switch (h2DataType) {
                  case "int": {
                    h2Statement.setObject(fieldIndex++, data.get(fieldName).intValue());
                    break;
                  }
                  case "varchar(128)": {
                    h2Statement.setObject(fieldIndex++, data.get(fieldName).textValue());
                    break;
                  }
                  default:
                    break;
                }
              }
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

  private static AwsCredentialsProvider getLocalAWSCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
  }

  @Test(enabled = false)
  public void testRecords()
      throws Exception {
    Assert.assertNotEquals(_totalRecordsPushedInStream, 0);

    ResultSet pinotResultSet =
        getPinotConnection().execute("SELECT * FROM " + getTableName() + " ORDER BY Origin LIMIT 10000")
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
    for (int i = 0; i < _h2FieldNameAndTypes.size(); i++) {
      columnToIndex.put(pinotResultSet.getColumnName(i), i);
    }

    while (h2ResultSet.next()) {

      for (String fieldNameAndDatatype : _h2FieldNameAndTypes) {
        String[] fieldNameAndDatatypeList = fieldNameAndDatatype.split(" ");
        String fieldName = fieldNameAndDatatypeList[0];
        String h2DataType = fieldNameAndDatatypeList[1];
        switch (h2DataType) {
          case "int": {
            int expectedValue = h2ResultSet.getInt(fieldName);
            int actualValue = pinotResultSet.getInt(row, columnToIndex.get(fieldName));
            Assert.assertEquals(expectedValue, actualValue);
            break;
          }
          case "varchar(128)": {
            String expectedValue = h2ResultSet.getString(fieldName);
            String actualValue = pinotResultSet.getString(row, columnToIndex.get(fieldName));
            Assert.assertEquals(expectedValue, actualValue);
            break;
          }
          default:
            break;
        }
      }

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

  @Test(enabled = false)
  public void testCountRecords() {
    long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName()).getResultSet(0).getLong(0);
    Assert.assertEquals(count, _totalRecordsPushedInStream);
  }

  public void createH2ConnectionAndTable()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
    _h2Connection.prepareCall("DROP TABLE IF EXISTS " + getTableName()).execute();
    _h2FieldNameAndTypes = new ArrayList<>();

    InputStream inputStream = RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

    String line;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      while ((line = br.readLine()) != null) {
        break;
      }
    } finally {
      inputStream.close();
    }

    if (StringUtils.isNotBlank(line)) {
      JsonNode dataObject = JsonUtils.stringToJsonNode(line);

      Iterator<Map.Entry<String, JsonNode>> fieldIterator = dataObject.fields();
      while (fieldIterator.hasNext()) {
        Map.Entry<String, JsonNode> field = fieldIterator.next();
        String fieldName = field.getKey();
        JsonNodeType fieldDataType = field.getValue().getNodeType();

        String h2DataType;

        switch (fieldDataType) {
          case NUMBER: {
            h2DataType = "int";
            break;
          }
          case STRING: {
            h2DataType = "varchar(128)";
            break;
          }
          case BOOLEAN: {
            h2DataType = "boolean";
            break;
          }
          default: {
            throw new UnsupportedDataTypeException(
                "Kinesis Integration test doesn't support datatype: " + fieldDataType.name());
          }
        }

        _h2FieldNameAndTypes.add(fieldName + " " + h2DataType);
      }
    }

    _h2Connection.prepareCall("CREATE TABLE " + getTableName() + "(" + StringUtil.join(",",
        _h2FieldNameAndTypes.toArray(new String[_h2FieldNameAndTypes.size()])) + ")").execute();
  }

  @AfterClass(enabled = false)
  public void tearDown()
      throws Exception {
    if (_skipTestNoDockerInstalled) {
      return;
    }

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

  public static class DockerInfoCommand extends Command {

    public void execute() {
      String dockerInfo = dockerExe.execute(Collections.singletonList("info"));

      if (dockerInfo.toLowerCase().contains("error")) {
        throw new IllegalStateException("Docker daemon is not running!");
      }
    }
  }
}
