/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.integration.tests;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.broker.broker.BrokerTestUtils;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka;
import com.linkedin.pinot.common.utils.CommonConstants.Server;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 *
 */
public abstract class ClusterTest extends ControllerTest {
  private static final String _success = "success";
  protected List<HelixBrokerStarter> _brokerStarters = new ArrayList<HelixBrokerStarter>();
  protected List<HelixServerStarter> _serverStarters = new ArrayList<HelixServerStarter>();
  protected static String partitioningKey = null;

  protected int getRealtimeSegmentFlushSize(boolean useLlc) {
    if (useLlc) {
      return 5000;
    } else {
      return 20000;
    }
  }

  protected void startBroker() {
    startBrokers(1);
  }

  protected void startBrokers(int brokerCount) {
    try {
      for (int i = 0; i < brokerCount; ++i) {
        final String helixClusterName = getHelixClusterName();
        Configuration configuration = BrokerTestUtils.getDefaultBrokerConfiguration();
        configuration.setProperty("pinot.broker.timeoutMs", 100 * 1000L);
        configuration.setProperty("pinot.broker.client.queryPort", Integer.toString(BROKER_PORT + i));
        configuration.setProperty("pinot.broker.routing.table.builder.class", "random");
        overrideBrokerConf(configuration);
        _brokerStarters.add(BrokerTestUtils.startBroker(helixClusterName, ZkStarter.DEFAULT_ZK_STR, configuration));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startServer() {
    startServers(1);
  }

  protected void startServers(int serverCount) {
    try {
      for (int i = 0; i < serverCount; i++) {
        Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, Server.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
            Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_ADMIN_API_PORT,
            Integer.toString(Integer.valueOf(Server.DEFAULT_ADMIN_API_PORT) - i));
        configuration.setProperty(Server.CONFIG_OF_NETTY_PORT,
            Integer.toString(Integer.valueOf(Helix.DEFAULT_SERVER_NETTY_PORT) + i));
        configuration.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
        overrideOfflineServerConf(configuration);
        _serverStarters.add(new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, configuration));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideOfflineServerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    for (HelixBrokerStarter brokerStarter : _brokerStarters) {
      BrokerTestUtils.stopBroker(brokerStarter);
    }
  }

  protected void stopServer() {
    for (HelixServerStarter helixServerStarter : _serverStarters) {
      if (helixServerStarter != null) {
        helixServerStarter.stop();
      }
    }
  }

  protected void addSchema(File schemaFile, String schemaName) throws Exception {
    FileUploadUtils.sendFile("localhost", ControllerTestUtils.DEFAULT_CONTROLLER_API_PORT, "schemas", schemaName,
        new FileInputStream(schemaFile), schemaFile.length(), FileUploadUtils.SendFileMethod.POST);
  }

  protected void updateSchema(File schemaFile, String schemaName) throws Exception {
    FileUploadUtils.sendFile("localhost", ControllerTestUtils.DEFAULT_CONTROLLER_API_PORT, "schemas/" + schemaName,
        schemaName, new FileInputStream(schemaFile), schemaFile.length(), FileUploadUtils.SendFileMethod.PUT);
  }

  protected void addOfflineTable(String timeColumnName, String timeColumnType, int retentionTimeValue,
      String retentionTimeUnit, String brokerTenant, String serverTenant, String tableName,
      SegmentVersion segmentVersion) throws Exception {
    addOfflineTable(timeColumnName, timeColumnType, retentionTimeValue, retentionTimeUnit, brokerTenant, serverTenant,
        new ArrayList<String>(), null, tableName, segmentVersion);
  }

  protected void addOfflineTable(String timeColumnName, String timeColumnType, int retentionTimeValue,
      String retentionTimeUnit, String brokerTenant, String serverTenant, List<String> invertedIndexColumns,
      String loadMode, String tableName, SegmentVersion segmentVersion)
          throws Exception {
    JSONObject request = ControllerRequestBuilder.buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant,
        timeColumnName, "DAYS", retentionTimeUnit, String.valueOf(retentionTimeValue), 3,
        "BalanceNumSegmentAssignmentStrategy", invertedIndexColumns, loadMode, segmentVersion.toString());
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
  }

  protected void updateOfflineTable(String timeColumnName, int retentionTimeValue, String retentionTimeUnit,
      String brokerTenant, String serverTenant, List<String> invertedIndexColumns, String loadMode, String tableName,
      SegmentVersion segmentVersion)
      throws Exception {
    JSONObject request =
        ControllerRequestBuilder.buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, timeColumnName,
            "DAYS", retentionTimeUnit, String.valueOf(retentionTimeValue), 3, "BalanceNumSegmentAssignmentStrategy",
            invertedIndexColumns, loadMode, segmentVersion.toString());
    sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forUpdateTableConfig(tableName),
        request.toString());
  }

  protected void dropOfflineTable(String tableName) throws Exception {
    sendDeleteRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableDelete(tableName + "_OFFLINE"));
  }

  protected void dropRealtimeTable(String tableName) throws Exception {
    sendDeleteRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableDelete(tableName + "_REALTIME"));
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements KafkaMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File avroFile;
    private org.apache.avro.Schema _avroSchema;
    private AvroRecordToPinotRowGenerator _rowGenerator;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName) throws Exception {
      // Load Avro schema
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
      _avroSchema = reader.getSchema();
      reader.close();
      _rowGenerator = new AvroRecordToPinotRowGenerator(indexingSchema);
      _reader = new GenericDatumReader<GenericData.Record>(_avroSchema);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.binaryDecoder(payload, offset, length, null));
        return _rowGenerator.transform(avroRecord, _avroSchema, destination);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected void addRealtimeTable(String tableName, String timeColumnName, String timeColumnType, int retentionDays,
      String retentionTimeUnit, String kafkaZkUrl, String kafkaTopic, String schemaName, String serverTenant,
      String brokerTenant, File avroFile, int realtimeSegmentFlushSize, String sortedColumn) throws Exception {
    List<String> invertedIndexColumns = Collections.emptyList();
    addRealtimeTable(tableName, timeColumnName, timeColumnType, retentionDays, retentionTimeUnit, kafkaZkUrl,
        kafkaTopic, schemaName, serverTenant, brokerTenant, avroFile, realtimeSegmentFlushSize, sortedColumn,
        invertedIndexColumns, null);
  }

  protected void addRealtimeTable(String tableName, String timeColumnName, String timeColumnType, int retentionDays,
      String retentionTimeUnit, String kafkaZkUrl, String kafkaTopic, String schemaName, String serverTenant,
      String brokerTenant, File avroFile, int realtimeSegmentFlushSize, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode)
      throws Exception {
    JSONObject streamConfig = new JSONObject();
    streamConfig.put("streamType", "kafka");
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, kafkaTopic);
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS,
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, kafkaZkUrl);
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, kafkaZkUrl);
    streamConfig.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(realtimeSegmentFlushSize));
    streamConfig.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET,
        "smallest");

    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;
    JSONObject request = ControllerRequestBuilder.buildCreateRealtimeTableJSON(tableName, serverTenant, brokerTenant,
        timeColumnName, timeColumnType, retentionTimeUnit, Integer.toString(retentionDays), 1,
        "BalanceNumSegmentAssignmentStrategy", streamConfig, schemaName, sortedColumn, invertedIndexColumns, null, true);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
  }

  protected void addLLCRealtimeTable(String tableName, String timeColumnName, String timeColumnType, int retentionDays,
      String retentionTimeUnit, String kafkaBrokerList, String kafkaTopic, String schemaName, String serverTenant,
      String brokerTenant, File avroFile, int realtimeSegmentFlushSize, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode)
      throws Exception {
    addLLCRealtimeTable(tableName, timeColumnName, timeColumnType, retentionDays,
    retentionTimeUnit, kafkaBrokerList, kafkaTopic, schemaName, serverTenant,
        brokerTenant, avroFile, realtimeSegmentFlushSize, sortedColumn,
        invertedIndexColumns, loadMode, null, new HashMap<String,String>());
  }

  protected void addLLCRealtimeTable(String tableName, String timeColumnName, String timeColumnType, int retentionDays,
      String retentionTimeUnit, String kafkaBrokerList, String kafkaTopic, String schemaName, String serverTenant,
      String brokerTenant, File avroFile, int realtimeSegmentFlushSize, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode, List<String> noDictionaryColumns,
      Map<String, String> partitioner)
          throws Exception {
    JSONObject metadata = new JSONObject();
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.simple.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, kafkaTopic);
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS,
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_BROKER_LIST, kafkaBrokerList);
    metadata.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(realtimeSegmentFlushSize));
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET,
        "smallest");
    metadata.put(DataSource.Realtime.SEGMENT_COMMIT_TIMEOUT_SECONDS, "3600");

    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;
    JSONObject request = ControllerRequestBuilder.buildCreateRealtimeTableJSON(tableName, serverTenant, brokerTenant,
        timeColumnName, timeColumnType, retentionTimeUnit, Integer.toString(retentionDays), 1,
        "BalanceNumSegmentAssignmentStrategy", metadata, schemaName, sortedColumn, invertedIndexColumns, null, false,
        noDictionaryColumns, partitioner);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
  }

  protected void addHybridTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, String schemaName, String serverTenant, String brokerTenant, File avroFile,
      String sortedColumn, List<String> invertedIndexColumns, String loadMode, boolean useLlc) throws Exception {
    int retentionDays = -1;
    String retentionTimeUnit = "";
    if (useLlc) {
      addLLCRealtimeTable(tableName, timeColumnName, timeColumnType, retentionDays, retentionTimeUnit, KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
          kafkaTopic, schemaName, serverTenant, brokerTenant, avroFile, getRealtimeSegmentFlushSize(useLlc),
          sortedColumn, invertedIndexColumns, loadMode);
    } else {
      addRealtimeTable(tableName, timeColumnName, timeColumnType, retentionDays, retentionTimeUnit, kafkaZkUrl,
          kafkaTopic, schemaName, serverTenant, brokerTenant, avroFile, getRealtimeSegmentFlushSize(useLlc),
          sortedColumn, invertedIndexColumns, loadMode);
    }
    addOfflineTable(timeColumnName, timeColumnType, retentionDays, retentionTimeUnit, brokerTenant, serverTenant,
        invertedIndexColumns, loadMode, tableName, SegmentVersion.v1);
  }

  protected void createBrokerTenant(String tenantName, int brokerCount) throws Exception {
    JSONObject request = ControllerRequestBuilder.buildBrokerTenantCreateRequestJSON(tenantName, brokerCount);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantCreate(),
        request.toString());
  }

  protected void createServerTenant(String tenantName, int offlineServerCount, int realtimeServerCount)
      throws Exception {
    JSONObject request = ControllerRequestBuilder.buildServerTenantCreateRequestJSON(tenantName,
        offlineServerCount + realtimeServerCount, offlineServerCount, realtimeServerCount);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forServerTenantCreate(),
        request.toString());
  }
}
