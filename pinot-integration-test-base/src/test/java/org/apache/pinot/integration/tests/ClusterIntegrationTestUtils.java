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
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tools.utils.ExplainPlanUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.testng.Assert;


public class ClusterIntegrationTestUtils {
  private ClusterIntegrationTestUtils() {
  }

  // Comparison limit
  public static final int MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE = 5;
  public static final int MAX_NUM_ROWS_TO_COMPARE = 10000;

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
            Assert.fail(
                String.format("Unsupported UNION Avro field: %s with underlying types: %s", fieldName, typesInUnion));
            break;
          case ARRAY:
            Schema.Type type = field.schema().getElementType().getType();
            Assert.assertTrue(isSingleValueAvroFieldType(type));
            // create Array data type based column.
            h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName, type, true, true));
            break;
          default:
            if (isSingleValueAvroFieldType(fieldType)) {
              h2FieldNameAndTypes.add(buildH2FieldNameAndType(fieldName, fieldType, false));
            } else {
              Assert.fail(String.format("Unsupported Avro field: %s with underlying types: %s", fieldName, fieldType));
            }
            break;
        }
      }

      h2Connection.prepareCall(String.format("DROP TABLE IF EXISTS %s", tableName)).execute();
      String columnsStr = StringUtil.join(",", h2FieldNameAndTypes.toArray(new String[0]));
      h2Connection.prepareCall(String.format("CREATE TABLE %s (%s)", tableName, columnsStr)).execute();
    }

    // Insert Avro records into H2 table
    String params = "?" + StringUtils.repeat(",?", h2FieldNameAndTypes.size() - 1);
    PreparedStatement h2Statement =
        h2Connection.prepareStatement(String.format("INSERT INTO %s VALUES (%s)", tableName, params));
    for (File avroFile : avroFiles) {
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        for (GenericRecord record : reader) {
          int h2Index = 1;
          for (int avroIndex = 0; avroIndex < numFields; avroIndex++) {
            Object value = record.get(avroIndex);
            if (value instanceof GenericData.Array) {
              GenericData.Array array = (GenericData.Array) value;
              Object[] arrayValue = new Object[MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE];
              for (int i = 0; i < MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
                if (i < array.size()) {
                  arrayValue[i] = array.get(i);
                  if (arrayValue[i] instanceof Utf8) {
                    arrayValue[i] =
                        StringUtil.sanitizeStringValue(arrayValue[i].toString(), FieldSpec.DEFAULT_MAX_LENGTH);
                  }
                } else {
                  arrayValue[i] = null;
                }
              }
              h2Statement.setObject(h2Index++, arrayValue);
            } else {
              if (value instanceof Utf8) {
                value = StringUtil.sanitizeStringValue(value.toString(), FieldSpec.DEFAULT_MAX_LENGTH);
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
    return buildH2FieldNameAndType(fieldName, avroFieldType, nullable, false);
  }

  /**
   * Helper method to build H2 field name and type.
   *
   * @param fieldName Field name
   * @param avroFieldType Avro field type
   * @param nullable Whether the column is nullable
   * @param arrayType Whether the column is array data type or not
   * @return H2 field name and type
   */
  private static String buildH2FieldNameAndType(String fieldName, Schema.Type avroFieldType, boolean nullable,
      boolean arrayType) {
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
    // if column is array data type, add Array with size.
    if (arrayType) {
      h2FieldType = String.format("%s  ARRAY[%d]", h2FieldType, MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
    }
    if (nullable) {
      return String.format("`%s` %s", fieldName, h2FieldType);
    } else {
      return String.format("`%s` %s not null", fieldName, h2FieldType);
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
    // Test segment with space and special character in the file name
    buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex + " %", segmentDir, tarDir);
  }

  /**
   * Builds one Pinot segment from the given Avro file.
   *
   * @param avroFile Avro file
   * @param tableConfig Pinot table config
   * @param schema Pinot schema
   * @param segmentNamePostfix Segment name postfix
   * @param segmentDir Output directory for the un-tarred segments
   * @param tarDir Output directory for the tarred segments
   */
  public static void buildSegmentFromAvro(File avroFile, TableConfig tableConfig,
      org.apache.pinot.spi.data.Schema schema, String segmentNamePostfix, File segmentDir, File tarDir)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(avroFile.getPath());
    segmentGeneratorConfig.setOutDir(segmentDir.getPath());
    segmentGeneratorConfig.setTableName(tableConfig.getTableName());
    segmentGeneratorConfig.setSegmentNamePostfix(segmentNamePostfix);

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
   * @param csvFile CSV File name
   * @param kafkaTopic Kafka topic
   * @param partitionColumnIndex Optional Index of the partition column
   * @throws Exception
   */
  public static void pushCsvIntoKafka(File csvFile, String kafkaTopic,
      @Nullable Integer partitionColumnIndex, boolean injectTombstones, StreamDataProducer producer)
      throws Exception {

    if (injectTombstones) {
      // publish lots of tombstones to livelock the consumer if it can't handle this properly
      for (int i = 0; i < 1000; i++) {
        // publish a tombstone first
        producer.produce(kafkaTopic, Longs.toByteArray(System.currentTimeMillis()), null);
      }
    }
    CSVFormat csvFormat = CSVFormat.DEFAULT.withSkipHeaderRecord(true);
    try (CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
      for (CSVRecord csv : parser) {
        byte[] keyBytes = (partitionColumnIndex == null) ? Longs.toByteArray(System.currentTimeMillis())
            : csv.get(partitionColumnIndex).getBytes(StandardCharsets.UTF_8);
        List<String> cols = new ArrayList<>();
        for (String col : csv) {
          cols.add(col);
        }
        byte[] bytes = String.join(",", cols).getBytes(StandardCharsets.UTF_8);
        producer.produce(kafkaTopic, keyBytes, bytes);
      }
    }
  }

  /**
   * Push the records from the given Avro files into a Kafka stream.
   *
   * @param csvRecords List of CSV record string
   * @param kafkaTopic Kafka topic
   * @param partitionColumnIndex Optional Index of the partition column
   * @throws Exception
   */
  public static void pushCsvIntoKafka(List<String> csvRecords, String kafkaTopic,
      @Nullable Integer partitionColumnIndex, boolean injectTombstones, StreamDataProducer producer)
      throws Exception {

    if (injectTombstones) {
      // publish lots of tombstones to livelock the consumer if it can't handle this properly
      for (int i = 0; i < 1000; i++) {
        // publish a tombstone first
        producer.produce(kafkaTopic, Longs.toByteArray(System.currentTimeMillis()), null);
      }
    }
    CSVFormat csvFormat = CSVFormat.DEFAULT.withSkipHeaderRecord(true);
    for (String recordCsv: csvRecords) {
      try (CSVParser parser = CSVParser.parse(recordCsv, csvFormat)) {
        for (CSVRecord csv : parser) {
          byte[] keyBytes = (partitionColumnIndex == null) ? Longs.toByteArray(System.currentTimeMillis())
              : csv.get(partitionColumnIndex).getBytes(StandardCharsets.UTF_8);
          List<String> cols = new ArrayList<>();
          for (String col : csv) {
            cols.add(col);
          }
          byte[] bytes = String.join(",", cols).getBytes(StandardCharsets.UTF_8);
          producer.produce(kafkaTopic, keyBytes, bytes);
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
   * @throws Exception
   */
  public static void pushAvroIntoKafka(List<File> avroFiles, String kafkaBroker, String kafkaTopic,
      int maxNumKafkaMessagesPerBatch, @Nullable byte[] header, @Nullable String partitionColumn,
      boolean injectTombstones)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");

    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      if (injectTombstones) {
        // publish lots of tombstones to livelock the consumer if it can't handle this properly
        for (int i = 0; i < 1000; i++) {
          // publish a tombstone first
          producer.produce(kafkaTopic, Longs.toByteArray(System.currentTimeMillis()), null);
        }
      }
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
   */
  static void testQuery(String pinotQuery, String queryResourceUrl, org.apache.pinot.client.Connection pinotConnection,
      String h2Query, Connection h2Connection)
      throws Exception {
    testQuery(pinotQuery, queryResourceUrl, pinotConnection, h2Query, h2Connection, null);
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   */
  static void testQuery(String pinotQuery, String queryResourceUrl, org.apache.pinot.client.Connection pinotConnection,
      String h2Query, Connection h2Connection, @Nullable Map<String, String> headers)
      throws Exception {
    testQuery(pinotQuery, queryResourceUrl, pinotConnection, h2Query, h2Connection, headers, null);
  }

  /**
   * Compare # of rows in pinot and H2 only. Succeed if # of rows matches. Note this only applies to non-aggregation
   * query.
   */
  static void testQueryWithMatchingRowCount(String pinotQuery, String queryResourceUrl,
      org.apache.pinot.client.Connection pinotConnection, String h2Query, Connection h2Connection,
      @Nullable Map<String, String> headers, @Nullable Map<String, String> extraJsonProperties)
      throws Exception {
    try {
      testQueryInternal(pinotQuery, queryResourceUrl, pinotConnection, h2Query, h2Connection, headers,
          extraJsonProperties, true, false);
    } catch (Exception e) {
      failure(pinotQuery, h2Query, e);
    }
  }

  static void testQuery(String pinotQuery, String queryResourceUrl, org.apache.pinot.client.Connection pinotConnection,
      String h2Query, Connection h2Connection, @Nullable Map<String, String> headers,
      @Nullable Map<String, String> extraJsonProperties) {
    try {
      testQueryInternal(pinotQuery, queryResourceUrl, pinotConnection, h2Query, h2Connection, headers,
          extraJsonProperties, false, false);
    } catch (Exception e) {
      failure(pinotQuery, h2Query, e);
    }
  }

  static void testQueryViaController(String pinotQuery, String queryResourceUrl,
      org.apache.pinot.client.Connection pinotConnection, String h2Query, Connection h2Connection,
      @Nullable Map<String, String> headers, @Nullable Map<String, String> extraJsonProperties) {
    try {
      testQueryInternal(pinotQuery, queryResourceUrl, pinotConnection, h2Query, h2Connection, headers,
          extraJsonProperties, false, true);
    } catch (Exception e) {
      failure(pinotQuery, h2Query, e);
    }
  }

  private static void testQueryInternal(String pinotQuery, String queryResourceUrl,
      org.apache.pinot.client.Connection pinotConnection, String h2Query, Connection h2Connection,
      @Nullable Map<String, String> headers, @Nullable Map<String, String> extraJsonProperties,
      boolean matchingRowCount, boolean viaController)
      throws Exception {
    // broker response
    JsonNode pinotResponse;
    if (viaController) {
      pinotResponse = ClusterTest.postQueryToController(pinotQuery, queryResourceUrl, headers, extraJsonProperties);
    } else {
      pinotResponse = ClusterTest.postQuery(pinotQuery, queryResourceUrl, headers, extraJsonProperties);
    }
    if (!pinotResponse.get("exceptions").isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse);
    }
    JsonNode brokerResponseRows = pinotResponse.get("resultTable").get("rows");
    long pinotNumRecordsSelected = pinotResponse.get("numDocsScanned").asLong();

    // connection response
    ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotQuery);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();

    // h2 response
    Assert.assertNotNull(h2Connection);
    Statement h2statement = h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(h2Query);
    ResultSet h2ResultSet = h2statement.getResultSet();

    // compare results
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    if (!QueryContextUtils.isAggregationQuery(queryContext)) {
      // selection/distinct

      Set<String> orderByColumns = new HashSet<>();
      if (queryContext.getOrderByExpressions() != null) {
        for (OrderByExpressionContext orderByExpression : queryContext.getOrderByExpressions()) {
          orderByExpression.getColumns(orderByColumns);
        }
      }
      Set<String> expectedValues = new HashSet<>();
      List<String> expectedOrderByValues = new ArrayList<>();
      int h2NumRows = getH2ExpectedValues(expectedValues, expectedOrderByValues, h2ResultSet, h2ResultSet.getMetaData(),
          orderByColumns);
      if (matchingRowCount) {
        if (numRows != h2NumRows) {
          throw new RuntimeException("Pinot # of rows " + numRows + " doesn't match h2 # of rows " + h2NumRows);
        } else {
          return;
        }
      }
      comparePinotResultsWithExpectedValues(expectedValues, expectedOrderByValues, resultTableResultSet, orderByColumns,
          pinotQuery, h2NumRows, pinotNumRecordsSelected);
    } else {
      if (queryContext.getGroupByExpressions() == null && !QueryContextUtils.isDistinctQuery(queryContext)) {
        // aggregation only

        // compare the single row
        h2ResultSet.first();
        for (int c = 0; c < numColumns; c++) {

          String h2Value = h2ResultSet.getString(c + 1);

          // If H2 value is null, it means no record selected in H2
          if (h2Value == null) {
            if (pinotNumRecordsSelected != 0) {
              throw new RuntimeException("No record selected in H2 but " + pinotNumRecordsSelected
                  + " records selected in Pinot, explain plan: " + getExplainPlan(pinotQuery, queryResourceUrl, headers,
                  extraJsonProperties));
            }

            // Skip further comparison
            return;
          }

          if (brokerResponseRows.size() == 0 || resultTableResultSet.getRowCount() == 0) {
            // Skip comparison if agg results returns 0 row, this is difference in treating
            // always-false literal predicate.
            return;
          }

          String brokerValue = brokerResponseRows.get(0).get(c).asText();
          String connectionValue = resultTableResultSet.getString(0, c);

          // Fuzzy compare expected value and actual value
          boolean error = fuzzyCompare(h2Value, brokerValue, connectionValue);
          if (error) {
            throw new RuntimeException(
                "Value: " + c + " does not match, expected: " + h2Value + ", got broker value: " + brokerValue
                    + ", got client value:" + connectionValue + ", explain plan: " + getExplainPlan(pinotQuery,
                    queryResourceUrl, headers, extraJsonProperties));
          }
        }
      } else {
        // aggregation group-by
        // TODO: compare results for aggregation group by queries w/o order by

        // Compare results for aggregation group by queries with order by
        if (queryContext.getOrderByExpressions() != null) {
          // don't compare query with multi-value column.
          if (h2Query.contains("_MV")) {
            return;
          }
          if (h2ResultSet.first()) {
            for (int i = 0; i < numRows; i++) {
              for (int c = 0; c < numColumns; c++) {
                String h2Value = h2ResultSet.getString(c + 1);
                String brokerValue = brokerResponseRows.get(i).get(c).asText();
                String connectionValue = resultTableResultSet.getString(i, c);
                boolean error = fuzzyCompare(h2Value, brokerValue, connectionValue);
                if (error) {
                  throw new RuntimeException(
                      "Value: " + c + " does not match, expected: " + h2Value + ", got broker value: " + brokerValue
                          + ", got client value:" + connectionValue + ", explain plan: " + getExplainPlan(pinotQuery,
                          queryResourceUrl, headers, extraJsonProperties));
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

  private static String getExplainPlan(String pinotQuery, String brokerUrl, @Nullable Map<String, String> headers,
      @Nullable Map<String, String> extraJsonProperties)
      throws Exception {
    JsonNode explainPlanForResponse =
        ClusterTest.postQuery("explain plan for " + pinotQuery, brokerUrl, headers, extraJsonProperties);
    return ExplainPlanUtils.formatExplainPlan(explainPlanForResponse);
  }

  private static int getH2ExpectedValues(Set<String> expectedValues, List<String> expectedOrderByValues,
      ResultSet h2ResultSet, ResultSetMetaData h2MetaData, Collection<String> orderByColumns)
      throws SQLException {
    Map<String, String> reusableExpectedValueMap = new HashMap<>();
    List<String> reusableColumnOrder = new ArrayList<>();
    int h2NumRows;
    int numColumns = h2MetaData.getColumnCount();

    for (h2NumRows = 0; h2ResultSet.next() && h2NumRows < MAX_NUM_ROWS_TO_COMPARE; h2NumRows++) {
      reusableExpectedValueMap.clear();
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
        int columnType = h2MetaData.getColumnType(columnIndex);
        if (columnType == Types.ARRAY) {
          // Multi-value column
          reusableColumnOrder.add(columnName);
          if (columnValue.contains(",")) {
            columnValue = Arrays.toString(
                Arrays.stream(columnValue.substring(1, columnValue.length() - 1).split(",")).map(String::trim).sorted()
                    .toArray());
          }
          reusableExpectedValueMap.put(columnName, columnValue);
        } else {
          // Single-value column
          String columnDataType = h2MetaData.getColumnTypeName(columnIndex);
          columnValue = removeTrailingZeroForNumber(columnValue, columnDataType);
          reusableExpectedValueMap.put(columnName, columnValue);
          reusableColumnOrder.add(columnName);
        }
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
      Set<String> orderByColumns, String pinotQuery, int h2NumRows, long pinotNumRecordsSelected) {

    int pinotNumRows = connectionResultSet.getRowCount();
    // No record selected in H2
    if (h2NumRows == 0) {
      if (pinotNumRows != 0) {
        throw new RuntimeException("No record selected in H2 but number of records selected in Pinot: " + pinotNumRows);
      }

      if (pinotNumRecordsSelected != 0) {
        throw new RuntimeException(
            "No selection result returned in Pinot but number of records selected: " + pinotNumRecordsSelected);
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
          } catch (IOException ignored) {
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
            actualValueBuilder.append(multiValue).append(' ');
          } else {
            // Single-value column
            actualValueBuilder.append(columnResult).append(' ');
          }
          if (orderByColumns.contains(columnName)) {
            actualOrderByValueBuilder.append(columnResult).append(' ');
          }
        }

        String actualValue = actualValueBuilder.toString();
        String actualOrderByValue = actualOrderByValueBuilder.toString();
        // Check actual value in expected values set, skip comparison if query response is truncated by limit
        if ((!isLimitSet || limit > h2NumRows) && !expectedValues.contains(actualValue)) {
          throw new RuntimeException(String.format(
              "Selection result differ in Pinot from H2: Pinot row: [ %s ] not found in H2 result set: [%s].",
              actualValue, expectedValues)
          );
        }
        if (!orderByColumns.isEmpty()) {
          // Check actual group value is the same as expected group value in the same order.
          if (!expectedOrderByValues.get(rowIndex).equals(actualOrderByValue)) {
            throw new RuntimeException(String.format(
                "Selection Order by result at row index: %d in Pinot: [ %s ] is different than result in H2: [ %s ].",
                rowIndex, actualOrderByValue, expectedOrderByValues.get(rowIndex)));
          }
        }
      }
    }
  }

  private static String removeTrailingZeroForNumber(String value, String type) {
    String upperCaseType = StringUtils.upperCase(type);
    // remove trailing zero after decimal point to compare decimal numbers with h2 data
    if (upperCaseType.equals("FLOAT") || upperCaseType.equals("DECFLOAT")
        || upperCaseType.equals("DOUBLE") || upperCaseType.equals("DOUBLE PRECISION")) {
      try {
        String result = (new BigDecimal(value)).stripTrailingZeros().toPlainString();
        return result + ".0";
      } catch (NumberFormatException ignored) {
        // ignoring the exception
      }
    }
    return value;
  }

  public static boolean isParsableDouble(String input) {
    try {
      Double.parseDouble(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean fuzzyCompare(String h2Value, String brokerValue, String connectionValue) {
    // Fuzzy compare expected value and actual value
    boolean error = false;
    if (isParsableDouble(h2Value)) {
      double expectedValue = Double.parseDouble(h2Value);
      double actualValueBroker = Double.parseDouble(brokerValue);
      double actualValueConnection = Double.parseDouble(connectionValue);
      double tolerance = Math.max(1.0, Math.abs(expectedValue * 1e-5));
      if (!DoubleMath.fuzzyEquals(actualValueBroker, expectedValue, tolerance) || !DoubleMath.fuzzyEquals(
          actualValueConnection, expectedValue, tolerance)) {
        error = true;
      }
    } else {
      if (!h2Value.equals(brokerValue) || !h2Value.equals(connectionValue)) {
        error = true;
      }
    }
    return error;
  }

  private static void failure(String pinotQuery, String h2Query, @Nullable Exception e) {
    String failureMessage = "Caught exception while testing query!";
    failure(pinotQuery, h2Query, failureMessage, e);
  }
  /**
   * Helper method to report failures.
   *
   * @param pinotQuery Pinot query
   * @param h2Query H2 query
   * @param failureMessage Failure message
   * @param e Exception
   */
  private static void failure(String pinotQuery, String h2Query, String failureMessage, @Nullable Exception e) {
    failureMessage += "\nPinot query: " + pinotQuery + "\nH2 query: " + h2Query;
    if (e == null) {
      Assert.fail(failureMessage);
    } else {
      Assert.fail(failureMessage, e);
    }
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
