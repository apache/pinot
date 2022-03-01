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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Function;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.activation.UnsupportedDataTypeException;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.integration.tests.rocketmq.MiniRocketMQCluster;
import org.apache.pinot.plugin.stream.rocketmq.RocketMQConfig;
import org.apache.pinot.plugin.stream.rocketmq.RocketMQConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(enabled = false)
public class RealtimeRocketMQIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeRocketMQIntegrationTest.class);

  protected MiniRocketMQCluster _rocketmqInstance;
  protected String _namesrvAddress;
  private long _totalRecordsPushedInStream = 0;
  List<String> _h2FieldNameAndTypes = new ArrayList<>();

  public static final int NUM_QUEUES = 10;
  public static final String SCHEMA_FILE_PATH = "rocketmq/airlineStats_data_reduced.schema";
  public static final String DATA_FILE_PATH = "rocketmq/airlineStats_data_reduced.json";

  @BeforeClass(enabled = false)
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start RocketMQ
    startRocketMQ();

    // Create and upload the schema and table config
    addSchema(createRocketMQSchema());
    addTableConfig(createRocketMQTableConfig());

    createH2ConnectionAndTable();

    // Push data into RocketMQ
    pushRecordsIntoRocketMQ();

    // Wait for all documents loaded
    waitForAllDocsLoadedRocketMQ(120_000L);
  }

  @Test(enabled = false)
  public void testRecords()
      throws Exception {
    Assert.assertNotEquals(_totalRecordsPushedInStream, 0);

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
    long count =
        getPinotConnection().execute(new Request("sql", "SELECT COUNT(*) FROM " + getTableName())).getResultSet(0)
            .getLong(0);

    Assert.assertEquals(count, _totalRecordsPushedInStream);
  }

  public void createH2ConnectionAndTable()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
    _h2Connection.prepareCall("DROP TABLE IF EXISTS " + getTableName()).execute();
    _h2FieldNameAndTypes = new ArrayList<>();

    InputStream inputStream =
        RealtimeRocketMQIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

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
                "RocketMQ Integration test doesn't support datatype: " + fieldDataType.name());
          }
        }

        _h2FieldNameAndTypes.add(fieldName + " " + h2DataType);
      }
    }

    _h2Connection.prepareCall("CREATE TABLE " + getTableName() + "(" + StringUtil
        .join(",", _h2FieldNameAndTypes.toArray(new String[_h2FieldNameAndTypes.size()])) + ")").execute();
  }

  @AfterClass(enabled = false)
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopRocketMQ();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  public Schema createRocketMQSchema()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(SCHEMA_FILE_PATH);
    Assert.assertNotNull(resourceUrl);
    return Schema.fromFile(new File(resourceUrl.getFile()));
  }

  protected void waitForAllDocsLoadedRocketMQ(long timeoutMs)
      throws Exception {
    waitForAllDocsLoadedRocketMQ(timeoutMs, true);
  }

  protected void waitForAllDocsLoadedRocketMQ(long timeoutMs, boolean raiseError) {
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

  public TableConfig createRocketMQTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getTableName())
        .setTimeColumnName("DaysSinceEpoch").setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setLLC(true).setStreamConfigs(createRocketMQStreamConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  protected Map<String, String> createRocketMQStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "rocketmq";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            getRocketMQTopic());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "30000");
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            StreamConfig.ConsumerType.HIGHLEVEL.toString());
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, RocketMQConfig.CONSUMER_PROP_NAMESPACE), getRocketMQNamespace());
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, RocketMQConfig.NAME_SERVER_LIST), getRocketMQNameServers());
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        RocketMQConsumerFactory.class.getName());
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(200));
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  protected String getRocketMQNamespace() {
    return "";
  }

  protected String getRocketMQNameServers() {
    return _namesrvAddress;
  }

  protected String getRocketMQTopic() {
    return getClass().getSimpleName();
  }

  protected void startRocketMQ()
      throws Exception {
    _rocketmqInstance = new MiniRocketMQCluster("integration_test_cluster", "default_broker");
    _rocketmqInstance.start();
    Thread.sleep(3000L);
    _namesrvAddress = _rocketmqInstance.getNamesrvAddress();
    _rocketmqInstance.initTopic(getRocketMQTopic(), NUM_QUEUES);
    Thread.sleep(3000L);
  }

  protected void stopRocketMQ() {
    if (_rocketmqInstance != null) {
      try {
        _rocketmqInstance.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  protected void pushRecordsIntoRocketMQ()
      throws Exception {
    DefaultMQProducer producer = new DefaultMQProducer("TEST_PRODUCER");
    producer.setNamesrvAddr(_namesrvAddress);
    producer.start();

    try {
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < _h2FieldNameAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + getTableName() + " VALUES (" + params.toString() + ")");

      InputStream inputStream =
          RealtimeRocketMQIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          JsonNode data = JsonUtils.stringToJsonNode(line);

          SendResult result = producer.send(new Message(getRocketMQTopic(), line.getBytes(StandardCharsets.UTF_8)));
          if (result.getSendStatus().equals(SendStatus.SEND_OK)) {
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

      inputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Could not publish records to RocketMQ Stream", e);
    }

    producer.shutdown();
  }
}
