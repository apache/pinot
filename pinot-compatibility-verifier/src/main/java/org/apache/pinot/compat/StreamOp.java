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
package org.apache.pinot.compat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * PRODUCE
 *   Produce events onto the stream, and verify that the number of rows in the tables increased
 *   by the number of rows produced. Also, verify the segment state for all replicas of the tables
 *
 * TODO: Consider using a file-based stream, where "pushing" events is simply adding new files to
 *       a folder named after the "stream". The implementation for the consumer would need to watch
 *       for new files and read them out. There could be one sub-folder per partition. This approach
 *       can save us handling kafka errors, etc.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamOp extends BaseOp {
  public enum Op {
    CREATE, PRODUCE
  }

  private Op _op;
  private String _streamConfigFileName;
  private int _numRows;
  private String _inputDataFileName;
  private String _tableConfigFileName;
  private String _recordReaderConfigFileName;
  private int _generationNumber;

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamOp.class);
  private static final String TOPIC_NAME = "topicName";
  private static final String NUM_PARTITIONS = "numPartitions";
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final String EXCEPTIONS = "exceptions";
  private static final String ERROR_CODE = "errorCode";
  private static final String NUM_SERVERS_QUERIED = "numServersQueried";
  private static final String NUM_SERVERS_RESPONEDED = "numServersResponded";
  private static final String TOTAL_DOCS = "totalDocs";
  private static final short KAFKA_REPLICATION_FACTOR = 1;

  public StreamOp() {
    super(OpType.STREAM_OP);
  }

  public Op getOp() {
    return _op;
  }

  public void setOp(Op op) {
    _op = op;
  }

  public String getStreamConfigFileName() {
    return _streamConfigFileName;
  }

  public void setStreamConfigFileName(String streamConfigFileName) {
    _streamConfigFileName = streamConfigFileName;
  }

  public int getNumRows() {
    return _numRows;
  }

  public void setNumRows(int numRows) {
    _numRows = numRows;
  }

  public String getInputDataFileName() {
    return _inputDataFileName;
  }

  public void setInputDataFileName(String inputDataFileName) {
    _inputDataFileName = inputDataFileName;
  }

  public String getTableConfigFileName() {
    return _tableConfigFileName;
  }

  public void setTableConfigFileName(String tableConfigFileName) {
    _tableConfigFileName = tableConfigFileName;
  }

  public String getRecordReaderConfigFileName() {
    return _recordReaderConfigFileName;
  }

  public void setRecordReaderConfigFileName(String recordReaderConfigFileName) {
    _recordReaderConfigFileName = recordReaderConfigFileName;
  }

  @Override
  boolean runOp(int generationNumber) {
    _generationNumber = generationNumber;
    switch (_op) {
      case CREATE:
        return createKafkaTopic();
      case PRODUCE:
        return produceData();
      default:
        return true;
    }
  }

  private boolean createKafkaTopic() {
    try {
      Properties streamConfigMap =
          JsonUtils.fileToObject(new File(getAbsoluteFileName(_streamConfigFileName)), Properties.class);
      String topicName = streamConfigMap.getProperty(TOPIC_NAME);
      int partitions = Integer.parseInt(streamConfigMap.getProperty(NUM_PARTITIONS));

      final Map<String, Object> config = new HashMap<>();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          ClusterDescriptor.getInstance().getDefaultHost() + ":" + ClusterDescriptor.getInstance().getKafkaPort());
      config.put(AdminClientConfig.CLIENT_ID_CONFIG, "Kafka2AdminClient-" + UUID.randomUUID().toString());
      config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
      AdminClient adminClient = KafkaAdminClient.create(config);
      NewTopic topic = new NewTopic(topicName, partitions, KAFKA_REPLICATION_FACTOR);
      adminClient.createTopics(Collections.singletonList(topic)).all().get();
    } catch (Exception e) {
      LOGGER.error("Failed to create Kafka topic with stream config file: {}", _streamConfigFileName, e);
      return false;
    }
    return true;
  }

  private boolean produceData() {
    try {
      // get kafka topic
      Properties streamConfigMap =
          JsonUtils.fileToObject(new File(getAbsoluteFileName(_streamConfigFileName)), Properties.class);
      String topicName = streamConfigMap.getProperty(TOPIC_NAME);
      String partitionColumn = streamConfigMap.getProperty(PARTITION_COLUMN);

      // get table config
      TableConfig tableConfig =
          JsonUtils.fileToObject(new File(getAbsoluteFileName(_tableConfigFileName)), TableConfig.class);
      String tableName = tableConfig.getTableName();
      long existingTotalDoc = 0;

      // get original rows
      existingTotalDoc = fetchExistingTotalDocs(tableName);
      LOGGER.warn(String.format("produceData existingTotalDoc %d", existingTotalDoc));

      // push csv data to kafka
      Properties publisherProps = new Properties();
      publisherProps.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
      publisherProps.put("serializer.class", "kafka.serializer.DefaultEncoder");
      publisherProps.put("request.required.acks", "1");
      StreamDataProducer producer =
          StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, publisherProps);

      // create a temp file to replace placeholder for input data file
      File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-compat-test-stream-op-" + UUID.randomUUID());
      localTempDir.deleteOnExit();
      File localReplacedCSVFile = new File(localTempDir, "replaced");
      FileUtils.forceMkdir(localTempDir);
      Utils.replaceContent(new File(getAbsoluteFileName(_inputDataFileName)), localReplacedCSVFile,
          GENERATION_NUMBER_PLACEHOLDER, String.valueOf(_generationNumber));

      CSVRecordReaderConfig recordReaderConfig = JsonUtils
          .fileToObject(new File(getAbsoluteFileName(_recordReaderConfigFileName)), CSVRecordReaderConfig.class);
      Set<String> columnNames = new HashSet<>();
      Collections.addAll(columnNames,
          recordReaderConfig.getHeader().split(Character.toString(recordReaderConfig.getDelimiter())));

      String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
      String schemaName = TableNameBuilder.extractRawTableName(tableName);
      String schemaString = ControllerTest
          .sendGetRequest(ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.getInstance().getControllerUrl())
              .forSchemaGet(schemaName));
      Schema schema = JsonUtils.stringToObject(schemaString, Schema.class);
      DateTimeFormatSpec dateTimeFormatSpec = schema.getSpecForTimeColumn(timeColumn).getFormatSpec();

      try (RecordReader csvRecordReader = RecordReaderFactory
          .getRecordReader(FileFormat.CSV, localReplacedCSVFile, columnNames, recordReaderConfig)) {
        int count = 0;
        while (count < _numRows) {
          if (!csvRecordReader.hasNext()) {
            csvRecordReader.rewind();
          }
          GenericRow genericRow = csvRecordReader.next();
          // add time column value
          genericRow.putValue(timeColumn, dateTimeFormatSpec.fromMillisToFormat(System.currentTimeMillis()));

          JsonNode messageJson = JsonUtils.stringToJsonNode(genericRow.toString());
          ObjectNode extractedJson = JsonUtils.newObjectNode();
          for (String key : genericRow.getFieldToValueMap().keySet()) {
            extractedJson.set(key, messageJson.get("fieldToValueMap").get(key));
          }

          if (partitionColumn == null) {
            producer.produce(topicName, extractedJson.toString().getBytes(UTF_8));
          } else {
            producer.produce(topicName, partitionColumn.getBytes(UTF_8),
                extractedJson.toString().getBytes(UTF_8));
          }
          count++;
        }
      }

      // verify number of rows increases as expected
      waitForDocsLoaded(tableName, existingTotalDoc + _numRows, 60_000L);
      LOGGER.info("Verified {} new rows in table: {}", _numRows, tableName);
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to ingest stream data", e);
      return false;
    }
  }

  private long fetchExistingTotalDocs(String tableName)
      throws Exception {
    String query = "SELECT count(*) FROM " + tableName;
    JsonNode response = Utils.postSqlQuery(query, ClusterDescriptor.getInstance().getBrokerUrl());
    LOGGER.warn(String.format("fetchExistingTotalDocs response %s", response));
    if (response == null) {
      String errorMsg = String.format("Failed to query Table: %s", tableName);
      LOGGER.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    if (response.has(EXCEPTIONS) && !response.get(EXCEPTIONS).isEmpty()) {
      String errorMsg =
          String.format("Failed when running query: '%s'; got exceptions:\n%s\n", query, response.toPrettyString());
      JsonNode exceptions = response.get(EXCEPTIONS);
      JsonNode errorCode = exceptions.get(ERROR_CODE);
      if (String.valueOf(QueryException.BROKER_INSTANCE_MISSING_ERROR).equals(String.valueOf(errorCode))
          && errorCode != null) {
        LOGGER.warn(errorMsg + ".Trying again");
        return 0;
      }
      LOGGER.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    if (response.has(NUM_SERVERS_QUERIED) && response.has(NUM_SERVERS_RESPONEDED)
        && response.get(NUM_SERVERS_QUERIED).asInt() > response.get(NUM_SERVERS_RESPONEDED).asInt()) {
      String errorMsg = String.format("Failed when running query: %s; the response contains partial results", query);
      LOGGER.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    if (!response.has(TOTAL_DOCS)) {
      String errorMsg = String.format("Failed when running query: %s; the response contains no docs", query);
      LOGGER.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return response.get(TOTAL_DOCS).asLong();
  }

  private void waitForDocsLoaded(String tableName, long targetDocs, long timeoutMs) {
    LOGGER.info("Wait Doc to load ...");
    AtomicLong loadedDocs = new AtomicLong(-1);
    TestUtils.waitForCondition(
        () -> {
          long existingTotalDocs = fetchExistingTotalDocs(tableName);
          loadedDocs.set(existingTotalDocs);
          return existingTotalDocs == targetDocs;
        }, 100L, timeoutMs,
        "Failed to load " + targetDocs + " documents. Found " + loadedDocs.get() + " instead", true,
        Duration.ofSeconds(1));
  }
}
