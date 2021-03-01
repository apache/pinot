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
import com.google.common.collect.Lists;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.requesthandler.PinotQueryParserFactory;
import org.apache.pinot.core.requesthandler.PinotQueryRequest;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.testng.Assert;


public class ClusterIntegrationTestUtils {
  // Comparison limit
  public static final int MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE = 5;
  public static final int MAX_NUM_ROWS_TO_COMPARE = 10000;
  public static final int H2_MULTI_VALUE_SUFFIX_LENGTH = 5;

  private static final Random RANDOM = new Random();

  /**
   * Set up an H2 table with records from the given Avro files inserted.
   *
   * @param avroFiles Avro files that contains the records to be inserted
   * @param tableName Name of the table to be created
   * @param h2Connection H2 connection
   * @throws Exception
   */
  @SuppressWarnings("SqlNoDataSourceInspection")
  public static void setUpH2TableWithAvro(List<File> avroFiles, String tableName, Connection h2Connection)
      throws Exception {
    int numFields;

    // Pick a sample Avro file to extract the H2 schema and create the H2 table
    File sampleAvroFile = avroFiles.get(0);
    List<String> h2FieldNameAndTypes = new ArrayList<>();
    try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(sampleAvroFile)) {
      List<Schema.Field> fields = reader.getSchema().getFields();
      numFields = fields.size();

      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case UNION:
            // For UNION field type, we support the following underlying types:
            // - One single single-value type
            // - One single-value type and one NULL type
            List<Schema> typesInUnion = field.schema().getTypes();
            if (typesInUnion.size() == 1) {
              Schema.Type type = typesInUnion.get(0).getType();
              Assert.assertTrue(isSingleValueAvroFieldType(type));
              h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName, type, false));
              break;
            }
            if (typesInUnion.size() == 2) {
              Schema.Type type = typesInUnion.get(0).getType();
              Assert.assertTrue(isSingleValueAvroFieldType(type));
              Assert.assertEquals(typesInUnion.get(1).getType(), Schema.Type.NULL);
              h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName, type, true));
              break;
            }
            Assert.fail("Unsupported UNION Avro field: " + fieldName + " with underlying types: " + typesInUnion);
            break;
          case ARRAY:
            Schema.Type type = field.schema().getElementType().getType();
            Assert.assertTrue(isSingleValueAvroFieldType(type));
            // Split multi-value field into MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE single-value fields
            for (int i = 0; i < MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
              h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName + "__MV" + i, type, true));
            }
            break;
          default:
            if (isSingleValueAvroFieldType(fieldType)) {
              h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName, fieldType, false));
            } else {
              Assert.fail("Unsupported Avro field: " + fieldName + " with type: " + fieldType);
            }
            break;
        }
      }

      h2Connection.prepareCall("DROP TABLE IF EXISTS " + tableName).execute();
      h2Connection.prepareCall("CREATE TABLE " + tableName + "(" + StringUtil
          .join(",", h2FieldNameAndTypes.toArray(new String[h2FieldNameAndTypes.size()])) + ")").execute();
    }

    // Insert Avro records into H2 table
    StringBuilder params = new StringBuilder("?");
    for (int i = 0; i < h2FieldNameAndTypes.size() - 1; i++) {
      params.append(",?");
    }
    PreparedStatement h2Statement =
        h2Connection.prepareStatement("INSERT INTO " + tableName + " VALUES (" + params.toString() + ")");
    for (File avroFile : avroFiles) {
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        for (GenericRecord record : reader) {
          int h2Index = 1;
          for (int avroIndex = 0; avroIndex < numFields; avroIndex++) {
            Object value = record.get(avroIndex);
            if (value instanceof GenericData.Array) {
              GenericData.Array array = (GenericData.Array) value;
              for (int i = 0; i < MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
                if (i < array.size()) {
                  value = array.get(i);
                  if (value instanceof Utf8) {
                    value = value.toString();
                  }
                } else {
                  value = null;
                }
                h2Statement.setObject(h2Index++, value);
              }
            } else {
              if (value instanceof Utf8) {
                value = value.toString();
              }
              h2Statement.setObject(h2Index++, value);
            }
          }
          h2Statement.execute();
        }
      }
    }
  }

  /**
   * Helper method to check whether the given Avro field type is a single value type (non-NULL).
   *
   * @param avroFieldType Avro field type
   * @return Whether the given Avro field type is a single value type (non-NULL)
   */
  private static boolean isSingleValueAvroFieldType(Schema.Type avroFieldType) {
    return (avroFieldType == Schema.Type.BOOLEAN) || (avroFieldType == Schema.Type.INT) || (avroFieldType
        == Schema.Type.LONG) || (avroFieldType == Schema.Type.FLOAT) || (avroFieldType == Schema.Type.DOUBLE) || (
        avroFieldType == Schema.Type.STRING);
  }

  /**
   * Helper method to build H2 field name and type.
   *
   * @param fieldName Field name
   * @param avroFieldType Avro field type
   * @param nullable Whether the column is nullable
   * @return H2 field name and type
   */
  private static String buildH2FieldNameAndType(String fieldName, Schema.Type avroFieldType, boolean nullable) {
    String avroFieldTypeName = avroFieldType.getName();
    String h2FieldType;
    switch (avroFieldTypeName) {
      case "int":
        h2FieldType = "bigint";
        break;
      case "string":
        h2FieldType = "varchar(128)";
        break;
      default:
        h2FieldType = avroFieldTypeName;
        break;
    }
    if (nullable) {
      return fieldName + " " + h2FieldType;
    } else {
      return fieldName + " " + h2FieldType + " not null";
    }
  }

  /**
   * Builds Pinot segments from the given Avro files. Each segment will be built using a separate thread.
   *
   * @param avroFiles List of Avro files
   * @param tableConfig Pinot table config
   * @param schema Pinot schema
   * @param baseSegmentIndex Base segment index number
   * @param segmentDir Output directory for the un-tarred segments
   * @param tarDir Output directory for the tarred segments
   */
  public static void buildSegmentsFromAvro(List<File> avroFiles, TableConfig tableConfig,
      org.apache.pinot.spi.data.Schema schema, int baseSegmentIndex, File segmentDir, File tarDir)
      throws Exception {
    int numAvroFiles = avroFiles.size();
    if (numAvroFiles == 1) {
      buildSegmentFromAvro(avroFiles.get(0), tableConfig, schema, baseSegmentIndex, segmentDir, tarDir);
    } else {
      ExecutorService executorService = Executors.newFixedThreadPool(numAvroFiles);
      List<Future<Void>> futures = new ArrayList<>(numAvroFiles);
      for (int i = 0; i < numAvroFiles; i++) {
        File avroFile = avroFiles.get(i);
        int segmentIndex = i + baseSegmentIndex;
        futures.add(executorService.submit(() -> {
          buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex, segmentDir, tarDir);
          return null;
        }));
      }
      executorService.shutdown();
      for (Future<Void> future : futures) {
        future.get();
      }
    }
  }

  /**
   * Builds one Pinot segment from the given Avro file.
   *
   * @param avroFile Avro file
   * @param tableConfig Pinot table config
   * @param schema Pinot schema
   * @param segmentIndex Segment index number
   * @param segmentDir Output directory for the un-tarred segments
   * @param tarDir Output directory for the tarred segments
   */
  public static void buildSegmentFromAvro(File avroFile, TableConfig tableConfig,
      org.apache.pinot.spi.data.Schema schema, int segmentIndex, File segmentDir, File tarDir)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(avroFile.getPath());
    segmentGeneratorConfig.setOutDir(segmentDir.getPath());
    segmentGeneratorConfig.setTableName(tableConfig.getTableName());
    // Test segment with space and special character in the file name
    segmentGeneratorConfig.setSegmentNamePostfix(segmentIndex + " %");

    // Build the segment
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    // Tar the segment
    String segmentName = driver.getSegmentName();
    File indexDir = new File(segmentDir, segmentName);
    File segmentTarFile = new File(tarDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);
  }

  /**
   * Push the records from the given Avro files into a Kafka stream.
   *
   * @param avroFiles List of Avro files
   * @param kafkaBroker Kafka broker config
   * @param kafkaTopic Kafka topic
   * @param maxNumKafkaMessagesPerBatch Maximum number of Kafka messages per batch
   * @param header Optional Kafka message header
   * @param partitionColumn Optional partition column
   * @throws Exception
   */
  public static void pushAvroIntoKafka(List<File> avroFiles, String kafkaBroker, String kafkaTopic,
      int maxNumKafkaMessagesPerBatch, @Nullable byte[] header, @Nullable String partitionColumn)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");

    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());
          for (GenericRecord genericRecord : reader) {
            outputStream.reset();
            if (header != null && 0 < header.length) {
              outputStream.write(header);
            }
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();

            byte[] keyBytes = (partitionColumn == null) ? Longs.toByteArray(System.currentTimeMillis())
                : (genericRecord.get(partitionColumn)).toString().getBytes();
            byte[] bytes = outputStream.toByteArray();
            producer.produce(kafkaTopic, keyBytes, bytes);
          }
        }
      }
    }
  }

  /**
   * Push the records from the given Avro files into a Kafka stream.
   *
   * @param avroFiles List of Avro files
   * @param kafkaBroker Kafka broker config
   * @param kafkaTopic Kafka topic
   * @param maxNumKafkaMessagesPerBatch Maximum number of Kafka messages per batch
   * @param header Optional Kafka message header
   * @param partitionColumn Optional partition column
   * @param commit if the transaction commits or aborts
   * @throws Exception
   */
  public static void pushAvroIntoKafkaWithTransaction(List<File> avroFiles, String kafkaBroker, String kafkaTopic,
      int maxNumKafkaMessagesPerBatch, @Nullable byte[] header, @Nullable String partitionColumn, boolean commit)
      throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBroker);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("request.required.acks", "1");
    props.put("transactional.id", "test-transaction");
    props.put("transaction.state.log.replication.factor", "2");

    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
    // initiate transaction.
    producer.initTransactions();
    producer.beginTransaction();
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());
          for (GenericRecord genericRecord : reader) {
            outputStream.reset();
            if (header != null && 0 < header.length) {
              outputStream.write(header);
            }
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();

            byte[] keyBytes = (partitionColumn == null) ? Longs.toByteArray(System.currentTimeMillis())
                : (genericRecord.get(partitionColumn)).toString().getBytes();
            byte[] bytes = outputStream.toByteArray();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord(kafkaTopic, keyBytes, bytes);
            producer.send(record);
          }
        }
      }
    }
    if (commit) {
      producer.commitTransaction();
    } else {
      producer.abortTransaction();
    }
  }


  /**
   * Push random generated
   *
   * @param avroFile Sample Avro file used to extract the Avro schema
   * @param kafkaBroker Kafka broker config
   * @param kafkaTopic Kafka topic
   * @param numKafkaMessagesToPush Number of Kafka messages to push
   * @param maxNumKafkaMessagesPerBatch Maximum number of Kafka messages per batch
   * @param header Optional Kafka message header
   * @param partitionColumn Optional partition column
   * @throws Exception
   */
  @SuppressWarnings("unused")
  public static void pushRandomAvroIntoKafka(File avroFile, String kafkaBroker, String kafkaTopic,
      int numKafkaMessagesToPush, int maxNumKafkaMessagesPerBatch, @Nullable byte[] header,
      @Nullable String partitionColumn)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");

    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
        Schema avroSchema = reader.getSchema();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        GenericRecord genericRecord = new GenericData.Record(avroSchema);

        while (numKafkaMessagesToPush > 0) {
          generateRandomRecord(genericRecord, avroSchema);

          outputStream.reset();
          if (header != null && 0 < header.length) {
            outputStream.write(header);
          }
          datumWriter.write(genericRecord, binaryEncoder);
          binaryEncoder.flush();

          byte[] keyBytes = (partitionColumn == null) ? Longs.toByteArray(System.currentTimeMillis())
              : (genericRecord.get(partitionColumn)).toString().getBytes();
          byte[] bytes = outputStream.toByteArray();

          producer.produce(kafkaTopic, keyBytes, bytes);
          numKafkaMessagesToPush--;
        }
      }
    }
  }

  /**
   * Helper method to generate random record.
   *
   * @param genericRecord Reusable generic record
   * @param avroSchema Avro schema
   */
  private static void generateRandomRecord(GenericRecord genericRecord, Schema avroSchema) {
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      Schema.Type fieldType = field.schema().getType();

      if (isSingleValueAvroFieldType(fieldType)) {
        genericRecord.put(fieldName, generateRandomValue(fieldType));
        continue;
      }
      if (fieldType == Schema.Type.UNION) {
        genericRecord.put(fieldName, generateRandomValue(field.schema().getTypes().get(0).getType()));
        continue;
      }
      if (fieldType == Schema.Type.ARRAY) {
        Schema.Type elementType = field.schema().getElementType().getType();
        int numValues = RANDOM.nextInt(5);
        List<Object> values = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
          values.add(generateRandomValue(elementType));
        }
        genericRecord.put(fieldName, values);
        continue;
      }

      throw new IllegalStateException("Unsupported field type: " + fieldType);
    }
  }

  /**
   * Helper method to generate random value for the given field type.
   *
   * @param fieldType Field type
   * @return Random value for the given field type
   */
  private static Object generateRandomValue(Schema.Type fieldType) {
    switch (fieldType) {
      case BOOLEAN:
        return RANDOM.nextBoolean();
      case INT:
        return RANDOM.nextInt(100000);
      case LONG:
        return RANDOM.nextLong() % 1000000;
      case FLOAT:
        return RANDOM.nextFloat() % 100000;
      case DOUBLE:
        return RANDOM.nextDouble() % 1000000;
      case STRING:
        return "potato" + RANDOM.nextInt(1000);
      default:
        throw new IllegalStateException("Unsupported field type: " + fieldType);
    }
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   * <p>LIMITATIONS:
   * <ul>
   *   <li>Skip comparison for selection and aggregation group-by when H2 results are too large to exhaust.</li>
   *   <li>Do not examine the order of result records.</li>
   * </ul>
   *
   * @param pinotQuery Pinot query
   * @param brokerUrl Pinot broker URL
   * @param pinotConnection Pinot connection
   * @param sqlQueries H2 SQL queries
   * @param h2Connection H2 connection
   * @throws Exception
   */
  public static void testPqlQuery(String pinotQuery, String brokerUrl,
      org.apache.pinot.client.Connection pinotConnection, @Nullable List<String> sqlQueries,
      @Nullable Connection h2Connection)
      throws Exception {
    // Use broker response for metadata check, connection response for value check
    PinotQueryRequest pinotBrokerQueryRequest = new PinotQueryRequest(CommonConstants.Broker.Request.PQL, pinotQuery);
    JsonNode pinotResponse = ClusterTest.postQuery(pinotBrokerQueryRequest, brokerUrl);
    Request pinotClientRequest = new Request(CommonConstants.Broker.Request.PQL, pinotQuery);
    ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotClientRequest);

    // Skip comparison if SQL queries are not specified
    if (sqlQueries == null) {
      return;
    }

    Assert.assertNotNull(h2Connection);
    Statement h2statement = h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    long pinotNumRecordsSelected = pinotResponse.get("numDocsScanned").asLong();

    // Aggregation results
    if (pinotResponse.has("aggregationResults")) {
      // Check number of aggregation results
      int numAggregationResults = pinotResultSetGroup.getResultSetCount();
      int numSqlQueries = sqlQueries.size();
      if (numAggregationResults != numSqlQueries) {
        String failureMessage =
            "Number of aggregation results: " + numAggregationResults + " does not match number of SQL queries: "
                + numSqlQueries;
        failure(pinotQuery, sqlQueries, failureMessage);
      }

      // Get aggregation type
      JsonNode pinotFirstAggregationResult = pinotResponse.get("aggregationResults").get(0);

      // Aggregation-only results
      if (pinotFirstAggregationResult.has("value")) {
        // Check over all aggregation functions
        for (int aggregationIndex = 0; aggregationIndex < numAggregationResults; aggregationIndex++) {
          // Get expected value for the aggregation
          h2statement.execute(sqlQueries.get(aggregationIndex));
          ResultSet h2ResultSet = h2statement.getResultSet();
          h2ResultSet.first();
          String h2Value = h2ResultSet.getString(1);

          // If H2 value is null, it means no record selected in H2
          if (h2Value == null) {
            if (pinotNumRecordsSelected != 0) {
              String failureMessage =
                  "No record selected in H2 but " + pinotNumRecordsSelected + " records selected in Pinot";
              failure(pinotQuery, sqlQueries, failureMessage);
            }

            // Skip further comparison
            return;
          }

          // Fuzzy compare expected value and actual value
          double expectedValue = Double.parseDouble(h2Value);
          String pinotValue = pinotResultSetGroup.getResultSet(aggregationIndex).getString(0);
          double actualValue = Double.parseDouble(pinotValue);
          if (!DoubleMath.fuzzyEquals(actualValue, expectedValue, 1.0)) {
            String failureMessage =
                "Value: " + aggregationIndex + " does not match, expected: " + h2Value + ", got: " + pinotValue;
            failure(pinotQuery, sqlQueries, failureMessage);
          }
        }

        return;
      }

      // Group-by results
      if (pinotFirstAggregationResult.has("groupByResult")) {
        // Get number of groups
        org.apache.pinot.client.ResultSet pinotFirstGroupByResultSet = pinotResultSetGroup.getResultSet(0);
        int pinotNumGroups = pinotFirstGroupByResultSet.getRowCount();

        // Get number of group keys in each group
        // If no group-by result returned by Pinot, set numGroupKeys to 0 since no comparison needed
        int pinotNumGroupKeys;
        if (pinotNumGroups == 0) {
          pinotNumGroupKeys = 0;
        } else {
          pinotNumGroupKeys = pinotFirstGroupByResultSet.getGroupKeyLength();
        }

        // Check over all aggregation functions
        for (int aggregationIndex = 0; aggregationIndex < numAggregationResults; aggregationIndex++) {
          // Construct expected result map from concatenated group keys to value
          h2statement.execute(sqlQueries.get(aggregationIndex));
          ResultSet h2ResultSet = h2statement.getResultSet();
          Map<String, String> expectedValues = new HashMap<>();
          int h2NumGroups;
          for (h2NumGroups = 0; h2ResultSet.next() && h2NumGroups < MAX_NUM_ROWS_TO_COMPARE; h2NumGroups++) {
            if (pinotNumGroupKeys != 0) {
              StringBuilder groupKey = new StringBuilder();
              for (int groupKeyIndex = 1; groupKeyIndex <= pinotNumGroupKeys; groupKeyIndex++) {
                // Convert boolean value to lower case
                groupKey.append(convertBooleanToLowerCase(h2ResultSet.getString(groupKeyIndex))).append(' ');
              }
              expectedValues.put(groupKey.toString(), h2ResultSet.getString(pinotNumGroupKeys + 1));
            }
          }

          // No record selected in H2
          if (h2NumGroups == 0) {
            if (pinotNumGroups != 0) {
              String failureMessage = "No group returned in H2 but " + pinotNumGroups + " groups returned in Pinot";
              failure(pinotQuery, sqlQueries, failureMessage);
            }

            // If the query has a HAVING clause and both H2 and Pinot have no groups, that is expected, so we don't need
            // to compare the number of docs scanned
            if (pinotQuery.contains("HAVING")) {
              return;
            }

            if (pinotNumRecordsSelected != 0) {
              String failureMessage = "No group returned in Pinot but " + pinotNumRecordsSelected + " records selected";
              failure(pinotQuery, sqlQueries, failureMessage);
            }

            // Skip further comparison
            return;
          }

          // Only compare exhausted results
          if (h2NumGroups < MAX_NUM_ROWS_TO_COMPARE) {
            // Check if all Pinot results are contained in the H2 results
            org.apache.pinot.client.ResultSet pinotGroupByResultSet =
                pinotResultSetGroup.getResultSet(aggregationIndex);
            for (int groupIndex = 0; groupIndex < pinotNumGroups; groupIndex++) {
              // Concatenate Pinot group keys
              StringBuilder groupKeyBuilder = new StringBuilder();
              for (int groupKeyIndex = 0; groupKeyIndex < pinotNumGroupKeys; groupKeyIndex++) {
                groupKeyBuilder.append(pinotGroupByResultSet.getGroupKeyString(groupIndex, groupKeyIndex)).append(' ');
              }
              String groupKey = groupKeyBuilder.toString();

              // Fuzzy compare expected value and actual value
              String h2Value = expectedValues.get(groupKey);
              if (h2Value == null) {
                String failureMessage = "Group returned in Pinot but not in H2: " + groupKey;
                failure(pinotQuery, sqlQueries, failureMessage);
                return;
              }
              double expectedValue = Double.parseDouble(h2Value);
              String pinotValue = pinotGroupByResultSet.getString(groupIndex);
              double actualValue = Double.parseDouble(pinotValue);
              if (!DoubleMath.fuzzyEquals(actualValue, expectedValue, 1.0)) {
                String failureMessage =
                    "Value: " + aggregationIndex + " does not match, expected: " + h2Value + ", got: " + pinotValue
                        + ", for group: " + groupKey;
                failure(pinotQuery, sqlQueries, failureMessage);
              }
            }
          }
        }

        return;
      }

      // Neither aggregation-only or group-by results
      String failureMessage = "Inside aggregation results, no aggregation-only or group-by results found";
      failure(pinotQuery, sqlQueries, failureMessage);
    }

    // Selection results
    if (pinotResponse.has("selectionResults")) {
      // Construct expected result set
      h2statement.execute(sqlQueries.get(0));
      ResultSet h2ResultSet = h2statement.getResultSet();
      ResultSetMetaData h2MetaData = h2ResultSet.getMetaData();

      // pinotResponse will have "selectionResults" in case of DISTINCT query too
      // so here we need to check if selection is null or not
      List<SelectionSort> sortSequence;
      BrokerRequest brokerRequest =
          PinotQueryParserFactory.get(CommonConstants.Broker.Request.PQL).compileToBrokerRequest(pinotQuery);
      if (brokerRequest.isSetSelections()) {
        sortSequence = brokerRequest.getSelections().getSelectionSortSequence();
      } else {
        sortSequence = new ArrayList<>();
      }

      List<String> orderByColumns;
      if (sortSequence == null) {
        orderByColumns = Collections.emptyList();
      } else {
        orderByColumns = new ArrayList<>();
        for (SelectionSort selectionSort : sortSequence) {
          orderByColumns.add(selectionSort.getColumn());
        }
      }
      Set<String> expectedValues = new HashSet<>();
      List<String> expectedOrderByValues = new ArrayList<>();

      int h2NumRows =
          getH2ExpectedValues(expectedValues, expectedOrderByValues, h2ResultSet, h2MetaData, orderByColumns);

      org.apache.pinot.client.ResultSet pinotSelectionResultSet = pinotResultSetGroup.getResultSet(0);

      // Only compare exhausted results
      comparePinotResultsWithExpectedValues(expectedValues, expectedOrderByValues, pinotSelectionResultSet,
          orderByColumns, pinotQuery, sqlQueries, h2NumRows, pinotNumRecordsSelected);
    } else {
      // Neither aggregation or selection results
      String failureMessage = "No aggregation or selection results found for query: " + pinotQuery;
      failure(pinotQuery, sqlQueries, failureMessage);
    }
  }

  /**
   * Run equivalent Pinot SQL and H2 query and compare the results.
   *
   * @param pinotQuery Pinot sql query
   * @param brokerUrl Pinot broker URL
   * @param pinotConnection Pinot connection
   * @param sqlQueries H2 SQL query
   * @param h2Connection H2 connection
   * @throws Exception
   */
  static void testSqlQuery(String pinotQuery, String brokerUrl, org.apache.pinot.client.Connection pinotConnection,
      @Nullable List<String> sqlQueries, @Nullable Connection h2Connection)
      throws Exception {
    if (pinotQuery == null || sqlQueries == null) {
      return;
    }

    BrokerRequest brokerRequest =
        PinotQueryParserFactory.get(CommonConstants.Broker.Request.SQL).compileToBrokerRequest(pinotQuery);

    List<String> orderByColumns = new ArrayList<>();
    if (isSelectionQuery(brokerRequest) && brokerRequest.getOrderBy() != null
        && brokerRequest.getOrderBy().size() > 0) {
      orderByColumns.addAll(CalciteSqlParser.extractIdentifiers(brokerRequest.getPinotQuery().getOrderByList(), false));
    }

    // broker response
    JsonNode pinotResponse = ClusterTest.postSqlQuery(pinotQuery, brokerUrl);
    if (pinotResponse.get("exceptions").size() > 0) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse);
    }
    JsonNode brokerResponseRows = pinotResponse.get("resultTable").get("rows");
    long pinotNumRecordsSelected = pinotResponse.get("numDocsScanned").asLong();

    // connection response
    Request pinotClientRequest = new Request("sql", pinotQuery);
    ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotClientRequest);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();

    // h2 response
    String sqlQuery = sqlQueries.get(0);
    Assert.assertNotNull(h2Connection);
    Statement h2statement = h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(sqlQuery);
    ResultSet h2ResultSet = h2statement.getResultSet();

    // compare results
    if (isSelectionQuery(brokerRequest)) { // selection
      Set<String> expectedValues = new HashSet<>();
      List<String> expectedOrderByValues = new ArrayList<>();
      int h2NumRows = getH2ExpectedValues(expectedValues, expectedOrderByValues, h2ResultSet, h2ResultSet.getMetaData(),
          orderByColumns);

      comparePinotResultsWithExpectedValues(expectedValues, expectedOrderByValues, resultTableResultSet, orderByColumns,
          pinotQuery, sqlQueries, h2NumRows, pinotNumRecordsSelected);
    } else { // aggregation
      if (!brokerRequest.isSetGroupBy()) { // aggregation only
        // compare the single row
        h2ResultSet.first();
        for (int c = 0; c < numColumns; c++) {

          String h2Value = h2ResultSet.getString(c + 1);

          // If H2 value is null, it means no record selected in H2
          if (h2Value == null) {
            if (pinotNumRecordsSelected != 0) {
              String failureMessage =
                  "No record selected in H2 but " + pinotNumRecordsSelected + " records selected in Pinot";
              failure(pinotQuery, Lists.newArrayList(sqlQuery), failureMessage);
            }

            // Skip further comparison
            return;
          }

          String brokerValue = brokerResponseRows.get(0).get(c).asText();
          String connectionValue = resultTableResultSet.getString(0, c);

          // Fuzzy compare expected value and actual value
          boolean error = fuzzyCompare(h2Value, brokerValue, connectionValue);
          if (error) {
            String failureMessage =
                "Value: " + c + " does not match, expected: " + h2Value + ", got broker value: " + brokerValue
                    + ", got client value:" + connectionValue;
            failure(pinotQuery, Lists.newArrayList(sqlQuery), failureMessage);
          }
        }
      } else { // aggregation group by
        // TODO: compare results for aggregation group by queries w/o order by

        // Compare results for aggregation group by queries with order by
        if (brokerRequest.getOrderBy() != null && brokerRequest.getOrderBy().size() > 0) {
          // don't compare query with multi-value column.
          if (sqlQuery.contains("_MV")) {
            return;
          }
          if (h2ResultSet.first()) {
            for (int i = 0; i < brokerResponseRows.size(); i++) {
              for (int c = 0; c < numColumns; c++) {
                String h2Value = h2ResultSet.getString(c + 1);
                String brokerValue = brokerResponseRows.get(i).get(c).asText();
                String connectionValue = resultTableResultSet.getString(i, c);
                boolean error = fuzzyCompare(h2Value, brokerValue, connectionValue);
                if (error) {
                  String failureMessage =
                      "Value: " + c + " does not match, expected: " + h2Value + ", got broker value: " + brokerValue
                          + ", got client value:" + connectionValue;
                  failure(pinotQuery, Lists.newArrayList(sqlQuery), failureMessage);
                }
              }
              if (!h2ResultSet.next()) {
                return;
              }
            }
          }
        }
      }
    }
  }

  private static boolean isSelectionQuery(BrokerRequest brokerRequest) {
    if (brokerRequest.getSelections() != null) {
      return true;
    }
    if (brokerRequest.getAggregationsInfo() != null && brokerRequest.getAggregationsInfo().get(0).getAggregationType()
        .equalsIgnoreCase("DISTINCT")) {
      return true;
    }
    return false;
  }

  private static void convertToUpperCase(List<String> columns) {
    for (int i = 0; i < columns.size(); i++) {
      columns.set(i, columns.get(i).toUpperCase());
    }
  }

  private static int getH2ExpectedValues(Set<String> expectedValues, List<String> expectedOrderByValues,
      ResultSet h2ResultSet, ResultSetMetaData h2MetaData, Collection<String> orderByColumns)
      throws SQLException {
    Map<String, String> reusableExpectedValueMap = new HashMap<>();
    Map<String, List<String>> reusableMultiValuesMap = new HashMap<>();
    List<String> reusableColumnOrder = new ArrayList<>();
    int h2NumRows;
    int numColumns = h2MetaData.getColumnCount();

    for (h2NumRows = 0; h2ResultSet.next() && h2NumRows < MAX_NUM_ROWS_TO_COMPARE; h2NumRows++) {
      reusableExpectedValueMap.clear();
      reusableMultiValuesMap.clear();
      reusableColumnOrder.clear();

      for (int columnIndex = 1; columnIndex <= numColumns; columnIndex++) { // h2 result set is 1-based
        String columnName = h2MetaData.getColumnName(columnIndex);

        // Handle null result and convert boolean value to lower case
        String columnValue = h2ResultSet.getString(columnIndex);
        if (columnValue == null) {
          columnValue = "null";
        } else {
          columnValue = convertBooleanToLowerCase(columnValue);
        }

        // Handle multi-value columns
        int length = columnName.length();
        if (length > H2_MULTI_VALUE_SUFFIX_LENGTH && columnName
            .substring(length - H2_MULTI_VALUE_SUFFIX_LENGTH, length - 1).equals("__MV")) {
          // Multi-value column
          String multiValueColumnName = columnName.substring(0, length - H2_MULTI_VALUE_SUFFIX_LENGTH);
          List<String> multiValue = reusableMultiValuesMap.get(multiValueColumnName);
          if (multiValue == null) {
            multiValue = new ArrayList<>();
            reusableMultiValuesMap.put(multiValueColumnName, multiValue);
            reusableColumnOrder.add(multiValueColumnName);
          }
          multiValue.add(columnValue);
        } else {
          // Single-value column
          String columnDataType = h2MetaData.getColumnTypeName(columnIndex);
          columnValue = removeTrailingZeroForNumber(columnValue, columnDataType);
          reusableExpectedValueMap.put(columnName, columnValue);
          reusableColumnOrder.add(columnName);
        }
      }

      // Add multi-value column results to the expected values
      // The reason for this step is that Pinot does not maintain order of elements in multi-value columns
      for (Map.Entry<String, List<String>> entry : reusableMultiValuesMap.entrySet()) {
        List<String> multiValue = entry.getValue();
        Collections.sort(multiValue);
        reusableExpectedValueMap.put(entry.getKey(), multiValue.toString());
      }

      // Build expected value String
      StringBuilder expectedValue = new StringBuilder();
      StringBuilder expectedOrderByValue = new StringBuilder();
      for (String column : reusableColumnOrder) {
        expectedValue.append(reusableExpectedValueMap.get(column)).append(' ');
        if (orderByColumns.contains(column)) {
          expectedOrderByValue.append(reusableExpectedValueMap.get(column)).append(' ');
        }
      }
      expectedValues.add(expectedValue.toString());
      expectedOrderByValues.add(expectedOrderByValue.toString());
    }

    return h2NumRows;
  }

  private static void comparePinotResultsWithExpectedValues(Set<String> expectedValues,
      List<String> expectedOrderByValues, org.apache.pinot.client.ResultSet connectionResultSet,
      Collection<String> orderByColumns, String pinotQuery, List<String> sqlQueries, int h2NumRows,
      long pinotNumRecordsSelected)
      throws IOException, SQLException {

    int pinotNumRows = connectionResultSet.getRowCount();
    // No record selected in H2
    if (h2NumRows == 0) {
      if (pinotNumRows != 0) {
        String failureMessage = "No record selected in H2 but number of records selected in Pinot: " + pinotNumRows;
        failure(pinotQuery, sqlQueries, failureMessage);
        return;
      }

      if (pinotNumRecordsSelected != 0) {
        String failureMessage =
            "No selection result returned in Pinot but number of records selected: " + pinotNumRecordsSelected;
        failure(pinotQuery, sqlQueries, failureMessage);
        return;
      }

      // Skip further comparison
      return;
    }

    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(pinotQuery);
    boolean isLimitSet = compiledQuery.isSetLimit();
    int limit = compiledQuery.getLimit();

    // Only compare exhausted results
    if (h2NumRows < MAX_NUM_ROWS_TO_COMPARE) {

      for (int rowIndex = 0; rowIndex < pinotNumRows; rowIndex++) {
        // Build actual value String.
        StringBuilder actualValueBuilder = new StringBuilder();
        StringBuilder actualOrderByValueBuilder = new StringBuilder();
        for (int columnIndex = 0; columnIndex < connectionResultSet.getColumnCount(); columnIndex++) {
          // Convert column name to all uppercase to make it compatible with H2
          String columnName = connectionResultSet.getColumnName(columnIndex).toUpperCase();
          String columnResult = connectionResultSet.getString(rowIndex, columnIndex);

          String columnDataType = connectionResultSet.getColumnDataType(columnIndex);
          columnResult = removeTrailingZeroForNumber(columnResult, columnDataType);

          JsonNode columnValues = null;
          try {
            columnValues = JsonUtils.stringToJsonNode(columnResult);
          } catch (IOException e) {
          }

          if (columnValues != null && columnValues.isArray()) {
            // Multi-value column
            List<String> multiValue = new ArrayList<>();
            int length = columnValues.size();
            for (int elementIndex = 0; elementIndex < length; elementIndex++) {
              multiValue.add(columnValues.get(elementIndex).asText());
            }
            for (int elementIndex = length; elementIndex < MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; elementIndex++) {
              multiValue.add("null");
            }
            Collections.sort(multiValue);
            actualValueBuilder.append(multiValue.toString()).append(' ');
            if (orderByColumns.contains(columnName)) {
              actualOrderByValueBuilder.append(columnResult).append(' ');
            }
          } else {
            // Single-value column
            actualValueBuilder.append(columnResult).append(' ');
            if (orderByColumns.contains(columnName)) {
              actualOrderByValueBuilder.append(columnResult).append(' ');
            }
          }
        }

        String actualValue = actualValueBuilder.toString();
        String actualOrderByValue = actualOrderByValueBuilder.toString();
        // Check actual value in expected values set, skip comparison if query response is truncated by limit
        if ((!isLimitSet || limit > h2NumRows) && !expectedValues.contains(actualValue)) {
          String failureMessage =
              "Selection result returned in Pinot but not in H2: " + actualValue + ", " + expectedValues;
          failure(pinotQuery, sqlQueries, failureMessage);
          return;
        }
        if (!orderByColumns.isEmpty()) {
          // Check actual group value is the same as expected group value in the same order.
          if (!expectedOrderByValues.get(rowIndex).equals(actualOrderByValue)) {
            String failureMessage = String.format(
                "Selection Order by result at row index: %d in Pinot: [ %s ] is different than result in H2: [ %s ].",
                rowIndex, actualOrderByValue, expectedOrderByValues.get(rowIndex));
            failure(pinotQuery, sqlQueries, failureMessage);
            return;
          }
        }
      }
    }
  }

  private static String removeTrailingZeroForNumber(String value, String type) {
    // remove trailing zero after decimal point to compare decimal numbers with h2 data
    if (type == null || type.toUpperCase().equals("FLOAT") || type.toUpperCase().equals("DOUBLE") || type.toUpperCase()
        .equals("BIGINT")) {
      try {
        return (new BigDecimal(value)).stripTrailingZeros().toPlainString();
      } catch (NumberFormatException e) {
      }
    }
    return value;
  }

  private static List<String> appendColumnsToSelectionRequests(Collection<String> columns, List<String> requests) {
    final int FIRST_COLUMN_INDEX = 7;
    List<String> resultRequests = new ArrayList<>();
    StringBuilder columnsString = new StringBuilder();
    for (String column : columns) {
      columnsString.append(column + ", ");
    }

    for (String request : requests) {
      String resultRequest = "Select " + columnsString + request.trim().substring(FIRST_COLUMN_INDEX);
      resultRequests.add(resultRequest);
    }
    return resultRequests;
  }

  private static boolean fuzzyCompare(String h2Value, String brokerValue, String connectionValue) {
    // Fuzzy compare expected value and actual value
    boolean error = false;
    if (NumberUtils.isParsable(h2Value)) {
      double expectedValue = Double.parseDouble(h2Value);
      double actualValueBroker = Double.parseDouble(brokerValue);
      double actualValueConnection = Double.parseDouble(connectionValue);
      if (!DoubleMath.fuzzyEquals(actualValueBroker, expectedValue, 1.0) || !DoubleMath
          .fuzzyEquals(actualValueConnection, expectedValue, 1.0)) {
        error = true;
      }
    } else {
      if (!h2Value.equals(brokerValue) || !h2Value.equals(connectionValue)) {
        error = true;
      }
    }
    return error;
  }

  /**
   * Helper method to report failures.
   *
   * @param pqlQuery Pinot PQL query
   * @param sqlQueries H2 SQL queries
   * @param failureMessage Failure message
   * @param e Exception
   */
  private static void failure(String pqlQuery, @Nullable List<String> sqlQueries, String failureMessage,
      @Nullable Exception e) {
    failureMessage += "\nPQL: " + pqlQuery;
    if (sqlQueries != null) {
      failureMessage += "\nSQL: " + sqlQueries;
    }
    if (e == null) {
      Assert.fail(failureMessage);
    } else {
      Assert.fail(failureMessage, e);
    }
  }

  /**
   * Helper method to report failures.
   *
   * @param pqlQuery Pinot PQL query
   * @param sqlQueries H2 SQL queries
   * @param failureMessage Failure message
   */
  private static void failure(String pqlQuery, @Nullable List<String> sqlQueries, String failureMessage) {
    failure(pqlQuery, sqlQueries, failureMessage, null);
  }

  /**
   * Helper method to convert boolean value to lower case.
   * <p>The reason for this method is that boolean values in H2 results are all uppercase characters, while in Pinot
   * they are all lowercase characters.
   * <p>If value is neither <code>TRUE</code> or <code>FALSE</code>, return itself.
   *
   * @param value raw value.
   * @return converted value.
   */
  private static String convertBooleanToLowerCase(String value) {
    if (value.equals("TRUE")) {
      return "true";
    }
    if (value.equals("FALSE")) {
      return "false";
    }
    return value;
  }
}
