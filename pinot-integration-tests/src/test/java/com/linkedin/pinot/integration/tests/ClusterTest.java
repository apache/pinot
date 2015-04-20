/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.broker.broker.BrokerTestUtils;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.*;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.*;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import java.io.File;
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
import org.testng.Assert;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 *
 * @author jfim
 */
public abstract class ClusterTest extends ControllerTest {
  private static final String _success = "success";
  protected HelixBrokerStarter _brokerStarter;
  protected HelixServerStarter _offlineServerStarter;

  protected void startBroker() {
    try {
      assert _brokerStarter == null;
      Configuration configuration = BrokerTestUtils.getDefaultBrokerConfiguration();
      overrideBrokerConf(configuration);
      _brokerStarter = BrokerTestUtils.startBroker(getHelixClusterName(), ZkTestUtils.DEFAULT_ZK_STR, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startServer() {
    try {
      Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
      overrideOfflineServerConf(configuration);
      _offlineServerStarter = new HelixServerStarter(getHelixClusterName(), ZkTestUtils.DEFAULT_ZK_STR, configuration);
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
    BrokerTestUtils.stopBroker(_brokerStarter);
    _brokerStarter = null;
  }

  protected void stopOfflineServer() {
    // Do nothing
  }

  protected void createOfflineResource(String resourceName, String timeColumnName, String timeColumnType) throws Exception {
    JSONObject payload = ControllerRequestBuilderUtil.buildCreateOfflineResourceJSON(resourceName, 1, 1);
    if (timeColumnName != null && timeColumnType != null) {
      payload = payload
          .put(DataSource.TIME_COLUMN_NAME, timeColumnName)
          .put(DataSource.TIME_TYPE, timeColumnType);
    }
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString().replaceAll("}$", ", \"metadata\":{}}"));
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements KafkaMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File avroFile;
    private org.apache.avro.Schema _avroSchema;
    private AvroRecordToPinotRowGenerator _rowGenerator;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName)
        throws Exception {
      // Load Avro schema
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
      _avroSchema = reader.getSchema();
      reader.close();
      _rowGenerator = new AvroRecordToPinotRowGenerator(indexingSchema);
      _reader = new GenericDatumReader<GenericData.Record>(_avroSchema);
    }

    @Override
    public GenericRow decode(byte[] payload) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.createBinaryDecoder(payload, 0, payload.length, null));
        return _rowGenerator.transform(avroRecord, _avroSchema);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected void createRealtimeResource(String resourceName, String timeColumnName, String timeColumnType, String kafkaZkUrl, String kafkaTopic, File avroFile)
      throws Exception {
    // Extract avro schema from the avro file and turn it into Pinot resource creation metadata JSON
    Schema schema = AvroUtils.extractSchemaFromAvro(avroFile);
    JSONObject metadata = new JSONObject();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      metadata.put("schema." + fieldName + ".isSingleValue", Boolean.toString(fieldSpec.isSingleValueField()));
      metadata.put("schema." + fieldName + ".dataType", fieldSpec.getDataType().toString());
      if (!fieldName.equals(timeColumnName)) {
        metadata.put("schema." + fieldName + ".fieldType", fieldSpec.getFieldType().toString());
      } else {
        metadata.put("schema." + fieldName + ".fieldType", "time");
        if ("daysSinceEpoch".equals(timeColumnType)) {
          metadata.put("schema." + fieldName + ".timeUnit", "DAYS");
        } else {
          assert false;
        }
      }
      metadata.put("schema." + fieldName + ".columnName", fieldName);
      metadata.put("schema." + fieldName + ".delimeter", ",");
    }

    // Add realtime parameters
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, kafkaTopic);
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS, AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, kafkaZkUrl);
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING,
        kafkaZkUrl + "-tracking");

    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;

    JSONObject payload = ControllerRequestBuilderUtil.buildCreateRealtimeResourceJSON(resourceName, 1, 1);
    if (timeColumnName != null && timeColumnType != null) {
      payload = payload
          .put(DataSource.TIME_COLUMN_NAME, timeColumnName)
          .put(DataSource.TIME_TYPE, timeColumnType);
    }
    payload.put("metadata", metadata);
    String res = sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));
  }

  protected void addTableToOfflineResource(String resourceName, String tableName, String timeColumnName, String timeColumnType) throws Exception {
    JSONObject payload = ControllerRequestBuilderUtil.createOfflineClusterAddTableToResource(resourceName, tableName).toJSON();
    if (timeColumnName != null && timeColumnType != null) {
      payload = payload
          .put(DataSource.TIME_COLUMN_NAME, timeColumnName)
          .put(DataSource.TIME_TYPE, timeColumnType);
    }
    String res =
        sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));
  }

  protected void addTableToRealtimeResource(String resourceName, String tableName, String timeColumnName, String timeColumnType) throws Exception {
    JSONObject payload = ControllerRequestBuilderUtil.createRealtimeClusterAddTableToResource(resourceName, tableName).toJSON();
    payload = payload.put(DataSource.RESOURCE_TYPE, ResourceType.REALTIME.toString());
    if (timeColumnName != null && timeColumnType != null) {
      payload = payload
          .put(DataSource.TIME_COLUMN_NAME, timeColumnName)
          .put(DataSource.TIME_TYPE, timeColumnType);
    }
    String res =
        sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));
  }
}
