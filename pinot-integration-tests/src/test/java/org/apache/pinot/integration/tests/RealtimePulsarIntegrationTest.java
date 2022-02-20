package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Function;
import java.io.BufferedReader;
import java.io.File;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.activation.UnsupportedDataTypeException;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.integration.tests.pulsar.PulsarStandaloneCluster;
import org.apache.pinot.plugin.stream.pulsar.PulsarConfig;
import org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory;
import org.apache.pinot.plugin.stream.pulsar.PulsarConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimePulsarIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimePulsarIntegrationTest.class);
  
  private static final String STREAM_NAME = "pulsar-test";
  private static final String STREAM_TYPE = "pulsar";
  public static final int NUM_PARTITION = 3;

  // So, this airlineStats data file consists of only few fields and rows from the original data
  public static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  public static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";
  public static final String CLIENT_ID = "clientId";
  private PulsarClient _pulsarClient;
  private PulsarStandaloneCluster _pulsarStandaloneCluster;
  private String _bootStrapServer;

  private long _totalRecordsPushedInStream = 0;

  List<String> _h2FieldNameAndTypes = new ArrayList<>();

  @BeforeClass(enabled = true)
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start Pulsar and Embedded Zookeeper
    startPulsar();

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    addSchema(createPulsarSchema());
    addTableConfig(createPulsarTableConfig());

    createH2ConnectionAndTable();

    // Push data into Kinesis
    publishRecordsToPulsarTopic();

    // Wait for all documents loaded
    waitForAllDocsLoadedPulsar(120_000L);
  }

  public Schema createPulsarSchema()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(SCHEMA_FILE_PATH);
    Assert.assertNotNull(resourceUrl);
    return Schema.fromFile(new File(resourceUrl.getFile()));
  }

  protected void waitForAllDocsLoadedPulsar(long timeoutMs)
      throws Exception {
    waitForAllDocsLoadedPulsar(timeoutMs, true);
  }

  protected void waitForAllDocsLoadedPulsar(long timeoutMs, boolean raiseError) {
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

  public TableConfig createPulsarTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getTableName())
        .setTimeColumnName("DaysSinceEpoch").setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setLLC(true).setStreamConfigs(createPulsarStreamConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  public Map<String, String> createPulsarStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);

    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        STREAM_NAME);

    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "30000");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.LOWLEVEL.toString());
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), PulsarConsumerFactory.class.getName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.BOOTSTRAP_SERVERS), _bootStrapServer);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(200));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  public void startPulsar()
      throws Exception {
    _pulsarStandaloneCluster = new PulsarStandaloneCluster();
    _pulsarStandaloneCluster.start();

    PulsarAdmin admin =
        PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + _pulsarStandaloneCluster.getAdminPort()).build();

    _bootStrapServer = "pulsar://localhost:" + _pulsarStandaloneCluster.getBrokerPort();

    _pulsarClient = PulsarClient.builder().serviceUrl(_bootStrapServer).build();

    admin.topics().createPartitionedTopic(STREAM_NAME, NUM_PARTITION);

  }

  public void stopPulsar() {
    if (_pulsarStandaloneCluster != null) {
      _pulsarStandaloneCluster.stop();
    }
  }

  private void publishRecordsToPulsarTopic() {
    try {
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < _h2FieldNameAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + getTableName() + " VALUES (" + params.toString() + ")");

      InputStream inputStream =
          RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

      Producer<String> producer =
          _pulsarClient.newProducer(
              org.apache.pulsar.client.api.Schema.STRING).topic(STREAM_NAME).enableBatching(true).batchingMaxMessages(20).batchingMaxPublishDelay(1000, TimeUnit.MILLISECONDS).create();
      
      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          final String ls = line;
          CompletableFuture<MessageId> fut = producer.sendAsync(line);
          fut.thenAccept(messageId -> {
            try {
              if (messageId != null) {
                _totalRecordsPushedInStream++;
                JsonNode data = JsonUtils.stringToJsonNode(ls);
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
            }catch (Exception e){

            }
          });



        }
      }

      inputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Could not publish records to Pulsar Stream", e);
    }
  }
  
  @Test(enabled = true)
  public void testConsumeFromEarliestOffset()
      throws Exception {
    Assert.assertNotEquals(_totalRecordsPushedInStream, 0);

    ResultSet pinotResultSet = getPinotConnection().execute(
        new Request("sql", "SELECT * FROM " + getTableName() + " ORDER BY Origin LIMIT 10000")).getResultSet(0);

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

  @Test(enabled = true)
  public void testConsumeFromStoredOffset()
      throws Exception {
    publishRecordsToPulsarTopic();
    waitForAllDocsLoadedPulsar(120000L);
    Assert.assertNotEquals(_totalRecordsPushedInStream, 0);

    ResultSet pinotResultSet = getPinotConnection().execute(
        new Request("sql", "SELECT * FROM " + getTableName() + " ORDER BY Origin LIMIT 10000")).getResultSet(0);

    Assert.assertNotEquals(pinotResultSet.getRowCount(), 0);
    Assert.assertEquals(pinotResultSet.getRowCount(), _totalRecordsPushedInStream);

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

  @Test(enabled = true)
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

  @AfterClass(enabled = true)
  public void tearDown()
      throws Exception {

    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopPulsar();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}