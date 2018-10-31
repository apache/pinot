/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.math.DoubleMath;
import com.google.common.primitives.Longs;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;


public class ClusterIntegrationTestUtils {
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
  public static void setUpH2TableWithAvro(@Nonnull List<File> avroFiles, @Nonnull String tableName,
      @Nonnull Connection h2Connection) throws Exception {
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
      h2Connection.prepareCall("CREATE TABLE " + tableName + "(" + StringUtil.join(",",
          h2FieldNameAndTypes.toArray(new String[h2FieldNameAndTypes.size()])) + ")").execute();
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
  private static boolean isSingleValueAvroFieldType(@Nonnull Schema.Type avroFieldType) {
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
  @Nonnull
  private static String buildH2FieldNameAndType(@Nonnull String fieldName, @Nonnull Schema.Type avroFieldType,
      boolean nullable) {
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
   * Builds segments from the given Avro files. Each segment will be built using a separate thread.
   *
   * @param avroFiles List of Avro files
   * @param baseSegmentIndex Base segment index number
   * @param segmentDir Output directory for the un-tarred segments
   * @param tarDir Output directory for the tarred segments
   * @param tableName Table name
   * @param createStarTreeIndex Whether to create Star-tree index
   * @param starTreeV2BuilderConfigs List of star-tree V2 builder configs
   * @param rawIndexColumns Columns to create raw index with
   * @param pinotSchema Pinot schema
   * @param executor Executor
   */
  public static void buildSegmentsFromAvro(List<File> avroFiles, int baseSegmentIndex, File segmentDir, File tarDir,
      String tableName, boolean createStarTreeIndex, @Nullable List<StarTreeV2BuilderConfig> starTreeV2BuilderConfigs,
      @Nullable List<String> rawIndexColumns, @Nullable com.linkedin.pinot.common.data.Schema pinotSchema,
      Executor executor) {
    int numSegments = avroFiles.size();
    for (int i = 0; i < numSegments; i++) {
      final File avroFile = avroFiles.get(i);
      final int segmentIndex = i + baseSegmentIndex;
      executor.execute(() -> {
        try {
          File outputDir = new File(segmentDir, "segment-" + segmentIndex);
          SegmentGeneratorConfig segmentGeneratorConfig =
              SegmentTestUtils.getSegmentGeneratorConfig(avroFile, outputDir, TimeUnit.DAYS, tableName, pinotSchema);

          // Test segment with space and special character in the file name
          segmentGeneratorConfig.setSegmentNamePostfix(String.valueOf(segmentIndex) + " %");

          // Cannot build star-tree V1 and V2 at same time
          if (starTreeV2BuilderConfigs != null) {
            segmentGeneratorConfig.setStarTreeV2BuilderConfigs(starTreeV2BuilderConfigs);
          } else {
            if (createStarTreeIndex) {
              segmentGeneratorConfig.enableStarTreeIndex(null);
            }
          }

          if (rawIndexColumns != null) {
            segmentGeneratorConfig.setRawIndexCreationColumns(rawIndexColumns);
          }

          // Build the segment
          SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
          driver.init(segmentGeneratorConfig);
          driver.build();

          // Tar the segment
          File[] files = outputDir.listFiles();
          Assert.assertNotNull(files);
          File segmentFile = files[0];
          String segmentName = segmentFile.getName();
          TarGzCompressionUtils.createTarGzOfDirectory(segmentFile.getAbsolutePath(),
              new File(tarDir, segmentName).getAbsolutePath());
        } catch (Exception e) {
          // Ignored
        }
      });
    }
  }

  /**
   * Builds segments from the given Avro files. Each segment will be built using a separate thread.
   *
   * @param avroFiles List of Avro files
   * @param baseSegmentIndex Base segment index number
   * @param segmentDir Output directory for the un-tarred segments
   * @param tarDir Output directory for the tarred segments
   * @param tableName Table name
   * @param executor Executor
   */
  public static void buildSegmentsFromAvro(List<File> avroFiles, int baseSegmentIndex, File segmentDir, File tarDir,
      String tableName, Executor executor) {
    buildSegmentsFromAvro(avroFiles, baseSegmentIndex, segmentDir, tarDir, tableName, false, null, null, null,
        executor);
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
  public static void pushAvroIntoKafka(@Nonnull List<File> avroFiles, @Nonnull String kafkaBroker,
      @Nonnull String kafkaTopic, int maxNumKafkaMessagesPerBatch, @Nullable byte[] header,
      @Nullable String partitionColumn) throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<byte[], byte[]> producer = new Producer<>(producerConfig);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());

          List<KeyedMessage<byte[], byte[]>> messagesToWrite = new ArrayList<>(maxNumKafkaMessagesPerBatch);
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
            KeyedMessage<byte[], byte[]> data = new KeyedMessage<>(kafkaTopic, keyBytes, bytes);

            messagesToWrite.add(data);

            // Send a batch of messages
            if (messagesToWrite.size() == maxNumKafkaMessagesPerBatch) {
              producer.send(messagesToWrite);
              messagesToWrite.clear();
            }
          }

          // Send last batch of messages
          producer.send(messagesToWrite);
        }
      }
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
  public static void pushRandomAvroIntoKafka(@Nonnull File avroFile, @Nonnull String kafkaBroker,
      @Nonnull String kafkaTopic, int numKafkaMessagesToPush, int maxNumKafkaMessagesPerBatch, @Nullable byte[] header,
      @Nullable String partitionColumn) throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<byte[], byte[]> producer = new Producer<>(producerConfig);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
        Schema avroSchema = reader.getSchema();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);

        List<KeyedMessage<byte[], byte[]>> messagesToWrite = new ArrayList<>(maxNumKafkaMessagesPerBatch);
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
          KeyedMessage<byte[], byte[]> data = new KeyedMessage<>(kafkaTopic, keyBytes, bytes);

          messagesToWrite.add(data);

          // Send a batch of messages
          if (messagesToWrite.size() == maxNumKafkaMessagesPerBatch) {
            producer.send(messagesToWrite);
            messagesToWrite.clear();
          }

          numKafkaMessagesToPush--;
        }

        // Send last batch of messages
        producer.send(messagesToWrite);
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
  @Nonnull
  private static Object generateRandomValue(@Nonnull Schema.Type fieldType) {
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
   * @param pqlQuery Pinot PQL query
   * @param brokerUrl Pinot broker URL
   * @param pinotConnection Pinot connection
   * @param sqlQueries H2 SQL queries
   * @param h2Connection H2 connection
   * @throws Exception
   */
  public static void testQuery(@Nonnull String pqlQuery, @Nonnull String brokerUrl,
      @Nonnull com.linkedin.pinot.client.Connection pinotConnection, @Nullable List<String> sqlQueries,
      @Nullable Connection h2Connection) throws Exception {
    // Use broker response for metadata check, connection response for value check
    JSONObject pinotResponse = ClusterTest.postQuery(pqlQuery, brokerUrl);
    ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pqlQuery);

    // Skip comparison if SQL queries are not specified
    if (sqlQueries == null) {
      return;
    }

    Assert.assertNotNull(h2Connection);
    Statement h2statement = h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    int pinotNumRecordsSelected = pinotResponse.getInt("numDocsScanned");

    // Aggregation results
    if (pinotResponse.has("aggregationResults")) {
      // Check number of aggregation results
      int numAggregationResults = pinotResultSetGroup.getResultSetCount();
      int numSqlQueries = sqlQueries.size();
      if (numAggregationResults != numSqlQueries) {
        String failureMessage =
            "Number of aggregation results: " + numAggregationResults + " does not match number of SQL queries: "
                + numSqlQueries;
        failure(pqlQuery, sqlQueries, failureMessage);
      }

      // Get aggregation type
      JSONObject pinotFirstAggregationResult = pinotResponse.getJSONArray("aggregationResults").getJSONObject(0);

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
              failure(pqlQuery, sqlQueries, failureMessage);
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
            failure(pqlQuery, sqlQueries, failureMessage);
          }
        }

        return;
      }

      // Group-by results
      if (pinotFirstAggregationResult.has("groupByResult")) {
        // Get number of groups
        com.linkedin.pinot.client.ResultSet pinotFirstGroupByResultSet = pinotResultSetGroup.getResultSet(0);
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
              failure(pqlQuery, sqlQueries, failureMessage);
            }

            // If the query has a HAVING clause and both H2 and Pinot have no groups, that is expected, so we don't need
            // to compare the number of docs scanned
            if (pqlQuery.contains("HAVING")) {
              return;
            }

            if (pinotNumRecordsSelected != 0) {
              String failureMessage = "No group returned in Pinot but " + pinotNumRecordsSelected + " records selected";
              failure(pqlQuery, sqlQueries, failureMessage);
            }

            // Skip further comparison
            return;
          }

          // Only compare exhausted results
          if (h2NumGroups < MAX_NUM_ROWS_TO_COMPARE) {
            // Check if all Pinot results are contained in the H2 results
            com.linkedin.pinot.client.ResultSet pinotGroupByResultSet =
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
                failure(pqlQuery, sqlQueries, failureMessage);
                return;
              }
              double expectedValue = Double.parseDouble(h2Value);
              String pinotValue = pinotGroupByResultSet.getString(groupIndex);
              double actualValue = Double.parseDouble(pinotValue);
              if (!DoubleMath.fuzzyEquals(actualValue, expectedValue, 1.0)) {
                String failureMessage =
                    "Value: " + aggregationIndex + " does not match, expected: " + h2Value + ", got: " + pinotValue
                        + ", for group: " + groupKey;
                failure(pqlQuery, sqlQueries, failureMessage);
              }
            }
          }
        }

        return;
      }

      // Neither aggregation-only or group-by results
      String failureMessage = "Inside aggregation results, no aggregation-only or group-by results found";
      failure(pqlQuery, sqlQueries, failureMessage);
    }

    // Selection results
    if (pinotResponse.has("selectionResults")) {
      // Construct expected result set
      h2statement.execute(sqlQueries.get(0));
      ResultSet h2ResultSet = h2statement.getResultSet();
      ResultSetMetaData h2MetaData = h2ResultSet.getMetaData();

      Set<String> expectedValues = new HashSet<>();
      Map<String, String> reusableExpectedValueMap = new HashMap<>();
      Map<String, List<String>> reusableMultiValuesMap = new HashMap<>();
      List<String> reusableColumnOrder = new ArrayList<>();
      int h2NumRows;
      for (h2NumRows = 0; h2ResultSet.next() && h2NumRows < MAX_NUM_ROWS_TO_COMPARE; h2NumRows++) {
        reusableExpectedValueMap.clear();
        reusableMultiValuesMap.clear();
        reusableColumnOrder.clear();

        int numColumns = h2MetaData.getColumnCount();
        for (int columnIndex = 1; columnIndex <= numColumns; columnIndex++) {
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
          if (length > 5 && columnName.substring(length - 5, length - 1).equals("__MV")) {
            // Multi-value column
            String multiValueColumnName = columnName.substring(0, length - 5);
            List<String> multiValue = reusableMultiValuesMap.get(multiValueColumnName);
            if (multiValue == null) {
              multiValue = new ArrayList<>();
              reusableMultiValuesMap.put(multiValueColumnName, multiValue);
              reusableColumnOrder.add(multiValueColumnName);
            }
            multiValue.add(columnValue);
          } else {
            // Single-value column
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
        for (String column : reusableColumnOrder) {
          expectedValue.append(column).append(':').append(reusableExpectedValueMap.get(column)).append(' ');
        }

        expectedValues.add(expectedValue.toString());
      }

      com.linkedin.pinot.client.ResultSet pinotSelectionResultSet = pinotResultSetGroup.getResultSet(0);
      int pinotNumRows = pinotSelectionResultSet.getRowCount();

      // No record selected in H2
      if (h2NumRows == 0) {
        if (pinotNumRows != 0) {
          String failureMessage = "No record selected in H2 but number of records selected in Pinot: " + pinotNumRows;
          failure(pqlQuery, sqlQueries, failureMessage);
          return;
        }

        if (pinotNumRecordsSelected != 0) {
          String failureMessage =
              "No selection result returned in Pinot but number of records selected: " + pinotNumRecordsSelected;
          failure(pqlQuery, sqlQueries, failureMessage);
          return;
        }

        // Skip further comparison
        return;
      }

      // Only compare exhausted results
      if (h2NumRows < MAX_NUM_ROWS_TO_COMPARE) {
        // Check that Pinot results are contained in the H2 results
        int numColumns = pinotSelectionResultSet.getColumnCount();

        for (int rowIndex = 0; rowIndex < pinotNumRows; rowIndex++) {
          // Build actual value String.
          StringBuilder actualValueBuilder = new StringBuilder();
          for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            // Convert column name to all uppercase to make it compatible with H2
            String columnName = pinotSelectionResultSet.getColumnName(columnIndex).toUpperCase();
            String columnResult = pinotSelectionResultSet.getString(rowIndex, columnIndex);

            // TODO: Find a better way to identify multi-value column
            if (columnResult.charAt(0) == '[') {
              // Multi-value column
              JSONArray columnValues = new JSONArray(columnResult);
              List<String> multiValue = new ArrayList<>();
              int length = columnValues.length();
              for (int elementIndex = 0; elementIndex < length; elementIndex++) {
                multiValue.add(columnValues.getString(elementIndex));
              }
              for (int elementIndex = length; elementIndex < MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE;
                  elementIndex++) {
                multiValue.add("null");
              }
              Collections.sort(multiValue);
              actualValueBuilder.append(columnName).append(':').append(multiValue.toString()).append(' ');
            } else {
              // Single-value column
              actualValueBuilder.append(columnName).append(':').append(columnResult).append(' ');
            }
          }
          String actualValue = actualValueBuilder.toString();

          // Check actual value in expected values set
          if (!expectedValues.contains(actualValue)) {
            String failureMessage = "Selection result returned in Pinot but not in H2: " + actualValue;
            failure(pqlQuery, sqlQueries, failureMessage);
            return;
          }
        }
      }
    } else {
      // Neither aggregation or selection results
      String failureMessage = "No aggregation or selection results found for query: " + pqlQuery;
      failure(pqlQuery, sqlQueries, failureMessage);
    }
  }

  /**
   * Helper method to report failures.
   *
   * @param pqlQuery Pinot PQL query
   * @param sqlQueries H2 SQL queries
   * @param failureMessage Failure message
   * @param e Exception
   */
  private static void failure(@Nonnull String pqlQuery, @Nullable List<String> sqlQueries,
      @Nonnull String failureMessage, @Nullable Exception e) {
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
  private static void failure(@Nonnull String pqlQuery, @Nullable List<String> sqlQueries,
      @Nonnull String failureMessage) {
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
  @Nonnull
  private static String convertBooleanToLowerCase(@Nonnull String value) {
    if (value.equals("TRUE")) {
      return "true";
    }
    if (value.equals("FALSE")) {
      return "false";
    }
    return value;
  }
}
