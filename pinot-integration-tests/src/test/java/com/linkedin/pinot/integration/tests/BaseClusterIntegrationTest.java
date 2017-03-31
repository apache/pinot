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

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.ByteArrayPartitioner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Shared implementation details of the cluster integration tests.
 */
public abstract class BaseClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClusterIntegrationTest.class);

  private static final AtomicInteger totalAvroRecordWrittenCount = new AtomicInteger(0);
  private static final boolean BATCH_KAFKA_MESSAGES = true;
  private static final int MAX_MESSAGES_PER_BATCH = 10000;

  // Maximum number of queries skipped when random select queries from hard coded query set.
  private static final int MAX_NUM_QUERIES_SKIPPED = 200;
  // Number of queries generated.
  private static final int GENERATED_QUERY_COUNT = 100;
  // Gather failed queries or failed immediately.
  private static final boolean GATHER_FAILED_QUERIES = false;

  private final File _failedQueriesFile = new File(getClass().getSimpleName() + "-failed.txt");
  private int _failedQueryCount = 0;
  private int _queryCount = 0;

  private com.linkedin.pinot.client.Connection _pinotConnection;

  protected Connection _connection;
  protected QueryGenerator _queryGenerator;
  protected static long TOTAL_DOCS = 115545L;

  public static final int MAX_ELEMENTS_IN_MULTI_VALUE = 5;
  public static final int MAX_COMPARISON_LIMIT = 10000;

  @BeforeMethod
  public void resetQueryCounts() {
    _failedQueryCount = 0;
    _queryCount = 0;
  }

  @AfterMethod
  public void checkFailedQueryCount() {
    if (GATHER_FAILED_QUERIES) {
      if (_failedQueryCount != 0) {
        try (PrintWriter printWriter = new PrintWriter(new FileWriter(_failedQueriesFile, true))) {
          printWriter.println("# " + _failedQueryCount + "/" + _queryCount + " queries did not match with H2");
        } catch (IOException e) {
          LOGGER.warn("Caught exception while writing to failed queries file", e);
        }
        Assert.fail("Queries have failed during this test, check" + _failedQueriesFile + " for details.");
      }
    }
  }

  /**
   * Helper method to report failures.
   *
   * @param pqlQuery Pinot PQL query.
   * @param sqlQueries H2 SQL queries.
   * @param failureMessage failure message.
   * @param e exception.
   */
  private void failure(String pqlQuery, @Nullable List<String> sqlQueries, String failureMessage,
      @Nullable Exception e) {
    if (GATHER_FAILED_QUERIES) {
      if (e == null) {
        saveFailedQuery(pqlQuery, sqlQueries, failureMessage);
      } else {
        saveFailedQuery(pqlQuery, sqlQueries, failureMessage, e.toString());
      }
    } else {
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
  }

  private void failure(String pqlQuery, @Nullable List<String> sqlQueries, String failureMessage) {
    failure(pqlQuery, sqlQueries, failureMessage, null);
  }

  /**
   * Helper method to save failed queries.
   *
   * @param pqlQuery Pinot PQL query.
   * @param sqlQueries H2 SQL queries.
   * @param messages failure messages.
   */
  private void saveFailedQuery(String pqlQuery, @Nullable List<String> sqlQueries, String... messages) {
    _failedQueryCount++;

    try (PrintWriter printWriter = new PrintWriter(new FileWriter(_failedQueriesFile, true))) {
      printWriter.println("PQL: " + pqlQuery);
      if (sqlQueries != null) {
        printWriter.println("SQL: " + sqlQueries);
      }
      printWriter.println("Failure Messages:");
      for (String message : messages) {
        printWriter.println(message);
      }
      printWriter.println();
    } catch (IOException e) {
      LOGGER.warn("Caught exception while writing failed query to file.", e);
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
   * @param pqlQuery Pinot PQL query.
   * @param sqlQueries H2 SQL queries.
   * @throws Exception
   */
  protected void runQuery(String pqlQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    try {
      _queryCount++;

      // Run the query.
      // TODO Use Pinot client API for this
      JSONObject response = postQuery(pqlQuery);

      // Check exceptions.
      JSONArray exceptions = response.getJSONArray("exceptions");
      if (exceptions.length() > 0) {
        String failureMessage = "Got exceptions: " + exceptions;
        failure(pqlQuery, sqlQueries, failureMessage, null);
        return;
      }

      // Check total docs.
      long numTotalDocs = response.getLong("totalDocs");
      if (numTotalDocs != TOTAL_DOCS) {
        String failureMessage =
            "Number of total documents does not match, expected: " + TOTAL_DOCS + ", got: " + numTotalDocs;
        failure(pqlQuery, sqlQueries, failureMessage, null);
        return;
      }

      // Skip comparison if SQL queries not specified.
      if (sqlQueries == null) {
        return;
      }

      // Check results.
      Statement h2statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      int numDocsScanned = response.getInt("numDocsScanned");
      if (response.has("aggregationResults")) {
        // Aggregation and Group-by results.

        // Get results type.
        JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
        int numAggregationResults = aggregationResultsArray.length();
        int numSqlQueries = sqlQueries.size();
        if (numAggregationResults != numSqlQueries) {
          String failureMessage =
              "Number of aggregation results: " + numAggregationResults + " does not match number of SQL queries: "
                  + numSqlQueries;
          failure(pqlQuery, sqlQueries, failureMessage);
        }
        JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);

        if (firstAggregationResult.has("value")) {
          // Aggregation results.

          // Check over all aggregation functions.
          for (int i = 0; i < numAggregationResults; i++) {
            // Get expected value for the aggregation.
            h2statement.execute(sqlQueries.get(i));
            ResultSet sqlResultSet = h2statement.getResultSet();
            sqlResultSet.first();
            String sqlValue = sqlResultSet.getString(1);

            // If SQL value is null, it means no record selected in H2.
            if (sqlValue == null) {
              // Check number of documents scanned is 0.
              if (numDocsScanned != 0) {
                String failureMessage =
                    "No record selected in H2 but number of records selected in Pinot: " + numDocsScanned;
                failure(pqlQuery, sqlQueries, failureMessage);
              }
              // Skip further comparison.
              return;
            }

            // Get actual value for the aggregation.
            String pqlValue = aggregationResultsArray.getJSONObject(i).getString("value");

            // Fuzzy compare expected value and actual value.
            double expectedValue = Double.parseDouble(sqlValue);
            double actualValue = Double.parseDouble(pqlValue);
            if (Math.abs(actualValue - expectedValue) >= 1.0) {
              String failureMessage = "Value: " + i + " does not match, expected: " + sqlValue + ", got: " + pqlValue;
              failure(pqlQuery, sqlQueries, failureMessage);
              return;
            }
          }
        } else if (firstAggregationResult.has("groupByResult")) {
          // Group-by results.

          // Get number of groups and number of group keys in each group.
          JSONArray firstGroupByResults = aggregationResultsArray.getJSONObject(0).getJSONArray("groupByResult");
          int numGroups = firstGroupByResults.length();
          // If no group-by result returned by Pinot, set numGroupKeys to 0 since no comparison needed.
          int numGroupKeys;
          if (numGroups == 0) {
            numGroupKeys = 0;
          } else {
            numGroupKeys = firstGroupByResults.getJSONObject(0).getJSONArray("group").length();
          }

          // Check over all aggregation functions.
          for (int i = 0; i < numAggregationResults; i++) {
            // Get number of group keys.
            JSONArray groupByResults = aggregationResultsArray.getJSONObject(i).getJSONArray("groupByResult");

            // Construct expected result map from group keys to value.
            h2statement.execute(sqlQueries.get(i));
            ResultSet sqlResultSet = h2statement.getResultSet();
            Map<String, String> expectedValues = new HashMap<>();
            int sqlNumGroups;
            for (sqlNumGroups = 0; sqlResultSet.next() && sqlNumGroups < MAX_COMPARISON_LIMIT; sqlNumGroups++) {
              if (numGroupKeys != 0) {
                StringBuilder groupKey = new StringBuilder();
                for (int groupKeyIndex = 1; groupKeyIndex <= numGroupKeys; groupKeyIndex++) {
                  // Convert boolean value to lower case.
                  groupKey.append(convertBooleanToLowerCase(sqlResultSet.getString(groupKeyIndex))).append(' ');
                }
                expectedValues.put(groupKey.toString(), sqlResultSet.getString(numGroupKeys + 1));
              }
            }

            if (sqlNumGroups == 0) {
              // No record selected in H2.

              // Check if no record selected in Pinot.
              if (numGroups != 0) {
                String failureMessage = "No group returned in H2 but number of groups returned in Pinot: " + numGroups;
                failure(pqlQuery, sqlQueries, failureMessage);
                return;
              }

              // Check if number of documents scanned is 0.
              if (numDocsScanned != 0) {
                String failureMessage = "No group returned in Pinot but number of records selected: " + numDocsScanned;
                failure(pqlQuery, sqlQueries, failureMessage);
              }

              // Skip further comparison.
              return;
            } else if (sqlNumGroups < MAX_COMPARISON_LIMIT) {
              // Only compare exhausted results.

              // Check that all Pinot results are contained in the H2 results.
              for (int resultIndex = 0; resultIndex < numGroups; resultIndex++) {
                // Fetch Pinot group keys.
                JSONObject groupByResult = groupByResults.getJSONObject(resultIndex);
                JSONArray group = groupByResult.getJSONArray("group");
                StringBuilder groupKeyBuilder = new StringBuilder();
                for (int groupKeyIndex = 0; groupKeyIndex < numGroupKeys; groupKeyIndex++) {
                  groupKeyBuilder.append(group.getString(groupKeyIndex)).append(' ');
                }
                String groupKey = groupKeyBuilder.toString();

                // Check if Pinot group keys contained in H2 results.
                if (!expectedValues.containsKey(groupKey)) {
                  String failureMessage = "Group returned in Pinot but not in H2: " + groupKey;
                  failure(pqlQuery, sqlQueries, failureMessage);
                  return;
                }

                // Fuzzy compare expected value and actual value.
                String sqlValue = expectedValues.get(groupKey);
                String pqlValue = groupByResult.getString("value");
                double expectedValue = Double.parseDouble(sqlValue);
                double actualValue = Double.parseDouble(pqlValue);
                if (Math.abs(actualValue - expectedValue) >= 1.0) {
                  String failureMessage =
                      "Value: " + i + " does not match, expected: " + sqlValue + ", got: " + pqlValue + ", for group: "
                          + groupKey;
                  failure(pqlQuery, sqlQueries, failureMessage);
                  return;
                }
              }
            } else {
              // Cannot get exhausted results.

              // Skip further comparison.
              LOGGER.debug("SQL: {} returned at least {} rows, skipping comparison.", sqlQueries.get(0),
                  MAX_COMPARISON_LIMIT);
              return;
            }
          }
        } else {
          // Neither aggregation or group-by results.
          String failureMessage = "Inside aggregation results, no aggregation or group-by results found";
          failure(pqlQuery, sqlQueries, failureMessage);
        }
      } else if (response.has("selectionResults")) {
        // Selection results.

        // Construct expected result set.
        h2statement.execute(sqlQueries.get(0));
        ResultSet sqlResultSet = h2statement.getResultSet();
        ResultSetMetaData sqlMetaData = sqlResultSet.getMetaData();

        Set<String> expectedValues = new HashSet<>();
        Map<String, String> reusableExpectedValueMap = new HashMap<>();
        Map<String, List<String>> reusableMultiValuesMap = new HashMap<>();
        List<String> reusableColumnOrder = new ArrayList<>();
        int numResults;
        for (numResults = 0; sqlResultSet.next() && numResults < MAX_COMPARISON_LIMIT; numResults++) {
          reusableExpectedValueMap.clear();
          reusableMultiValuesMap.clear();
          reusableColumnOrder.clear();

          int numColumns = sqlMetaData.getColumnCount();
          for (int i = 1; i <= numColumns; i++) {
            String columnName = sqlMetaData.getColumnName(i);

            // Handle null result and convert boolean value to lower case.
            String columnValue = sqlResultSet.getString(i);
            if (columnValue == null) {
              columnValue = "null";
            } else {
              columnValue = convertBooleanToLowerCase(columnValue);
            }

            // Handle multi-value columns.
            int length = columnName.length();
            if (length > 5 && columnName.substring(length - 5, length - 1).equals("__MV")) {
              // Multi-value column.
              String multiValueColumnName = columnName.substring(0, length - 5);
              List<String> multiValue = reusableMultiValuesMap.get(multiValueColumnName);
              if (multiValue == null) {
                multiValue = new ArrayList<>();
                reusableMultiValuesMap.put(multiValueColumnName, multiValue);
                reusableColumnOrder.add(multiValueColumnName);
              }
              multiValue.add(columnValue);
            } else {
              // Single-value column.
              reusableExpectedValueMap.put(columnName, columnValue);
              reusableColumnOrder.add(columnName);
            }
          }

          // Add multi-value column results to the expected values.
          // The reason for this step is that Pinot does not maintain order of elements in multi-value columns.
          for (Map.Entry<String, List<String>> entry : reusableMultiValuesMap.entrySet()) {
            List<String> multiValue = entry.getValue();
            Collections.sort(multiValue);
            reusableExpectedValueMap.put(entry.getKey(), multiValue.toString());
          }

          // Build expected value String.
          StringBuilder expectedValue = new StringBuilder();
          for (String column : reusableColumnOrder) {
            expectedValue.append(column).append(':').append(reusableExpectedValueMap.get(column)).append(' ');
          }

          expectedValues.add(expectedValue.toString());
        }

        JSONObject selectionColumnsAndResults = response.getJSONObject("selectionResults");
        JSONArray selectionColumns = selectionColumnsAndResults.getJSONArray("columns");
        JSONArray selectionResults = selectionColumnsAndResults.getJSONArray("results");
        int numSelectionResults = selectionResults.length();

        if (numResults == 0) {
          // No record selected in H2.

          // Check if no record selected in Pinot.
          if (numSelectionResults != 0) {
            String failureMessage =
                "No record selected in H2 but number of records selected in Pinot: " + numSelectionResults;
            failure(pqlQuery, sqlQueries, failureMessage);
            return;
          }

          // Check if number of documents scanned is 0.
          if (numDocsScanned != 0) {
            String failureMessage =
                "No selection result returned in Pinot but number of records selected: " + numDocsScanned;
            failure(pqlQuery, sqlQueries, failureMessage);
          }
        } else if (numResults < MAX_COMPARISON_LIMIT) {
          // Only compare exhausted results.

          // Check that Pinot results are contained in the H2 results.
          int numColumns = selectionColumns.length();
          for (int i = 0; i < numSelectionResults; i++) {
            // Build actual value String.
            StringBuilder actualValueBuilder = new StringBuilder();
            JSONArray selectionResult = selectionResults.getJSONArray(i);

            for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
              // Convert column name to all uppercase to make it compatible with H2.
              String columnName = selectionColumns.getString(columnIndex).toUpperCase();

              Object columnResult = selectionResult.get(columnIndex);
              if (columnResult instanceof JSONArray) {
                // Multi-value column.
                JSONArray columnResultsArray = (JSONArray) columnResult;
                List<String> multiValue = new ArrayList<>();
                int length = columnResultsArray.length();
                for (int elementIndex = 0; elementIndex < length; elementIndex++) {
                  multiValue.add(columnResultsArray.getString(elementIndex));
                }
                for (int elementIndex = length; elementIndex < MAX_ELEMENTS_IN_MULTI_VALUE; elementIndex++) {
                  multiValue.add("null");
                }
                Collections.sort(multiValue);
                actualValueBuilder.append(columnName).append(':').append(multiValue.toString()).append(' ');
              } else {
                // Single-value column.
                actualValueBuilder.append(columnName).append(':').append((String) columnResult).append(' ');
              }
            }
            String actualValue = actualValueBuilder.toString();

            // Check actual value in expected values set.
            if (!expectedValues.contains(actualValue)) {
              String failureMessage = "Selection result returned in Pinot but not in H2: " + actualValue;
              failure(pqlQuery, sqlQueries, failureMessage);
              return;
            }
          }
        } else {
          // Cannot get exhausted results.
          LOGGER.debug("SQL: {} returned at least {} rows, skipping comparison.", sqlQueries.get(0),
              MAX_COMPARISON_LIMIT);
        }
      } else {
        // Neither aggregation or selection results.
        String failureMessage = "No aggregation or selection results found for query: " + pqlQuery;
        failure(pqlQuery, sqlQueries, failureMessage);
      }
    } catch (Exception e) {
      String failureMessage = "Caught exception while running query.";
      failure(pqlQuery, sqlQueries, failureMessage, e);
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

  public static void createH2SchemaAndInsertAvroFiles(List<File> avroFiles, Connection connection) {
    try {
      connection.prepareCall("DROP TABLE IF EXISTS mytable");
      File schemaAvroFile = avroFiles.get(0);
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(schemaAvroFile, datumReader);

      Schema schema = dataFileReader.getSchema();
      List<Schema.Field> fields = schema.getFields();
      List<String> columnNamesAndTypes = new ArrayList<String>(fields.size());
      int columnCount = 0;
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case UNION:
            List<Schema> types = field.schema().getTypes();
            String columnNameAndType;
            String typeName = types.get(0).getName();

            if (typeName.equalsIgnoreCase("int")) {
              typeName = "bigint";
            }

            if (types.size() == 1) {
              columnNameAndType = fieldName + " " + typeName + " not null";
            } else {
              columnNameAndType = fieldName + " " + typeName;
            }

            columnNamesAndTypes.add(columnNameAndType.replace("string", "varchar(128)"));
            ++columnCount;
            break;
          case ARRAY:
            String elementTypeName = field.schema().getElementType().getName();

            if (elementTypeName.equalsIgnoreCase("int")) {
              elementTypeName = "bigint";
            }

            elementTypeName = elementTypeName.replace("string", "varchar(128)");

            for (int i = 0; i < MAX_ELEMENTS_IN_MULTI_VALUE; i++) {
              columnNamesAndTypes.add(fieldName + "__MV" + i + " " + elementTypeName);
            }
            ++columnCount;
            break;
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            String fieldTypeName = fieldType.getName();

            if (fieldTypeName.equalsIgnoreCase("int")) {
              fieldTypeName = "bigint";
            }

            columnNameAndType = fieldName + " " + fieldTypeName + " not null";

            columnNamesAndTypes.add(columnNameAndType.replace("string", "varchar(128)"));
            ++columnCount;
            break;
          case RECORD:
            // Ignore records
            continue;
          default:
            // Ignore other avro types
            LOGGER.warn("Ignoring field {} of type {}", fieldName, field.schema());
        }
      }

      connection.prepareCall(
          "create table mytable("
              + StringUtil.join(",", columnNamesAndTypes.toArray(new String[columnNamesAndTypes.size()])) + ")")
          .execute();
      long start = System.currentTimeMillis();
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < columnNamesAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement statement =
          connection.prepareStatement("INSERT INTO mytable VALUES (" + params.toString() + ")");

      dataFileReader.close();

      for (File avroFile : avroFiles) {
        datumReader = new GenericDatumReader<GenericRecord>();
        dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
          record = dataFileReader.next(record);
          int jdbcIndex = 1;
          for (int avroIndex = 0; avroIndex < columnCount; ++avroIndex) {
            Object value = record.get(avroIndex);
            if (value instanceof GenericData.Array) {
              GenericData.Array array = (GenericData.Array) value;
              for (int i = 0; i < MAX_ELEMENTS_IN_MULTI_VALUE; i++) {
                if (i < array.size()) {
                  value = array.get(i);
                  if (value instanceof Utf8) {
                    value = value.toString();
                  }
                } else {
                  value = null;
                }
                statement.setObject(jdbcIndex, value);
                ++jdbcIndex;
              }
            } else {
              if (value instanceof Utf8) {
                value = value.toString();
              }
              statement.setObject(jdbcIndex, value);
              ++jdbcIndex;
            }
          }
          statement.execute();
        }
        dataFileReader.close();
      }
      LOGGER.info("Insertion took " + (System.currentTimeMillis() - start));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void pushAvroIntoKafka(List<File> avroFiles, String kafkaBroker, String kafkaTopic) {
    pushAvroIntoKafka(avroFiles, kafkaBroker, kafkaTopic, null);
  }

  public static void pushAvroIntoKafka(List<File> avroFiles, String kafkaBroker, String kafkaTopic, final byte[] header) {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    properties.put("partitioner.class","kafka.producer.ByteArrayPartitioner");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);
    for (File avroFile : avroFiles) {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536);
        DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
        BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(reader.getSchema());
        int recordCount = 0;
        List<KeyedMessage<byte[], byte[]>> messagesToWrite = new ArrayList<KeyedMessage<byte[], byte[]>>(10000);
        int messagesInThisBatch = 0;
        for (GenericRecord genericRecord : reader) {
          outputStream.reset();
          if (header != null && 0 < header.length) {
            outputStream.write(header);
          }
          datumWriter.write(genericRecord, binaryEncoder);
          binaryEncoder.flush();

          byte[] bytes = outputStream.toByteArray();
          byte[] keyBytes = (partitioningKey == null) ? Longs.toByteArray(System.currentTimeMillis()) :
              ((Long) genericRecord.get(partitioningKey)).toString().getBytes();
          KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(kafkaTopic,
              keyBytes, bytes);

          if (BATCH_KAFKA_MESSAGES) {
            messagesToWrite.add(data);
            messagesInThisBatch++;
            if (MAX_MESSAGES_PER_BATCH <= messagesInThisBatch) {
              LOGGER.debug("Sending a batch of {} records to Kafka", messagesInThisBatch);
              messagesInThisBatch = 0;
              producer.send(messagesToWrite);
              messagesToWrite.clear();
            }
          } else {
            producer.send(data);
          }
          recordCount += 1;
        }

        if (BATCH_KAFKA_MESSAGES) {
          LOGGER.info("Sending last match of {} records to Kafka", messagesToWrite.size());
          producer.send(messagesToWrite);
        }

        outputStream.close();
        reader.close();
        LOGGER.info("Finished writing " + recordCount + " records from " + avroFile.getName() + " into Kafka topic "
            + kafkaTopic + " from file " + avroFile.getName());
        int totalRecordCount = totalAvroRecordWrittenCount.addAndGet(recordCount);
        LOGGER.info("Total records written so far " + totalRecordCount);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public static void pushRandomAvroIntoKafka(File avroFile, String kafkaBroker, String kafkaTopic, int rowCount,
      Random random) {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<String, byte[]> producer = new Producer<String, byte[]>(producerConfig);
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536);
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
      BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
      Schema avroSchema = reader.getSchema();
      GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
      int recordCount = 0;

      int rowsRemaining = rowCount;
      int messagesInThisBatch = 0;
      while (rowsRemaining > 0) {
        int rowsInThisBatch = Math.min(rowsRemaining, MAX_MESSAGES_PER_BATCH);
        List<KeyedMessage<String, byte[]>> messagesToWrite =
            new ArrayList<KeyedMessage<String, byte[]>>(rowsInThisBatch);
        GenericRecord genericRecord = new GenericData.Record(avroSchema);

        for (int i = 0; i < rowsInThisBatch; ++i) {
          generateRandomRecord(genericRecord, avroSchema, random);
          outputStream.reset();
          datumWriter.write(genericRecord, binaryEncoder);
          binaryEncoder.flush();

          byte[] bytes = outputStream.toByteArray();
          KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(kafkaTopic, bytes);

          if (BATCH_KAFKA_MESSAGES) {
            messagesToWrite.add(data);
            messagesInThisBatch++;
            if (MAX_MESSAGES_PER_BATCH <= messagesInThisBatch) {
              messagesInThisBatch = 0;
              producer.send(messagesToWrite);
              messagesToWrite.clear();
              Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
          } else {
            producer.send(data);
          }
          recordCount += 1;
        }

        if (BATCH_KAFKA_MESSAGES) {
          producer.send(messagesToWrite);
        }

//        System.out.println("rowsRemaining = " + rowsRemaining);
        rowsRemaining -= rowsInThisBatch;
      }

      outputStream.close();
      reader.close();
      LOGGER.info(
          "Finished writing " + recordCount + " records from " + avroFile.getName() + " into Kafka topic " + kafkaTopic);
      int totalRecordCount = totalAvroRecordWrittenCount.addAndGet(recordCount);
      LOGGER.info("Total records written so far " + totalRecordCount);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private static void generateRandomRecord(GenericRecord genericRecord, Schema avroSchema, Random random) {
    for (Schema.Field field : avroSchema.getFields()) {
      Schema.Type fieldType = field.schema().getType();

      // Non-nullable single value?
      if (fieldType != Schema.Type.ARRAY && fieldType != Schema.Type.UNION) {
        switch(fieldType) {
          case INT:
            genericRecord.put(field.name(), random.nextInt(100000));
            break;
          case LONG:
            genericRecord.put(field.name(), random.nextLong() % 1000000L);
            break;
          case STRING:
            genericRecord.put(field.name(), "potato" + random.nextInt(1000));
            break;
          default:
            throw new RuntimeException("Unimplemented random record generation for field " + field);
        }
      } else if (fieldType == Schema.Type.UNION) { // Nullable field?
        // Use first type of union to determine actual data type
        switch(field.schema().getTypes().get(0).getType()) {
          case INT:
            genericRecord.put(field.name(), random.nextInt(100000));
            break;
          case LONG:
            genericRecord.put(field.name(), random.nextLong() % 1000000L);
            break;
          case STRING:
            genericRecord.put(field.name(), "potato" + random.nextInt(1000));
            break;
          default:
            throw new RuntimeException("Unimplemented random record generation for field " + field);
        }
      } else {
        // Multivalue field
        final int MAX_MULTIVALUES = 5;
        int multivalueCount = random.nextInt(MAX_MULTIVALUES);
        List<Object> values = new ArrayList<>(multivalueCount);

        switch(field.schema().getElementType().getType()) {
          case INT:
            for (int i = 0; i < multivalueCount; i++) {
              values.add(random.nextInt(100000));
            }
            break;
          case LONG:
            for (int i = 0; i < multivalueCount; i++) {
              values.add(random.nextLong() % 1000000L);
            }
            break;
          case STRING:
            for (int i = 0; i < multivalueCount; i++) {
              values.add("potato" + random.nextInt(1000));
            }
            break;
          default:
            throw new RuntimeException("Unimplemented random record generation for field " + field);
        }

        genericRecord.put(field.name(), values);
      }
    }
  }

  public static Future<Map<File, File>> buildSegmentsFromAvro(final List<File> avroFiles, Executor executor, int baseSegmentIndex,
      final File baseDirectory, final File segmentTarDir, final String tableName, final boolean createStarTreeIndex,
      final com.linkedin.pinot.common.data.Schema inputPinotSchema) {
    int segmentCount = avroFiles.size();
    LOGGER.info("Building " + segmentCount + " segments in parallel");
    List<ListenableFutureTask<Pair<File, File>>> futureTasks = new ArrayList<ListenableFutureTask<Pair<File,File>>>();

    for (int i = 1; i <= segmentCount; ++i) {
      final int segmentIndex = i - 1;
      final int segmentNumber = i + baseSegmentIndex;

      final ListenableFutureTask<Pair<File, File>> buildSegmentFutureTask =
          ListenableFutureTask.<Pair<File, File>>create(new Callable<Pair<File, File>>() {
        @Override
        public Pair<File, File> call() throws Exception {
          try {
            // Build segment
            LOGGER.info("Starting to build segment " + segmentNumber);
            File outputDir = new File(baseDirectory, "segment-" + segmentNumber);
            final File inputAvroFile = avroFiles.get(segmentIndex);
            final SegmentGeneratorConfig genConfig = SegmentTestUtils
                .getSegmentGenSpecWithSchemAndProjectedColumns(inputAvroFile, outputDir, TimeUnit.DAYS, tableName, inputPinotSchema);

            if (inputPinotSchema != null) {
              genConfig.setSchema(inputPinotSchema);
            }

            // jfim: We add a space and a special character to do a regression test for PINOT-3296 Segments with spaces
            // in their filename don't work properly
            genConfig.setSegmentNamePostfix(Integer.toString(segmentNumber) + " %");
            genConfig.setEnableStarTreeIndex(createStarTreeIndex);

            // Enable off heap star tree format in the integration test.
            StarTreeIndexSpec starTreeIndexSpec = null;
            if (createStarTreeIndex) {
              starTreeIndexSpec = new StarTreeIndexSpec();
              starTreeIndexSpec.setEnableOffHeapFormat(true);
            }
            genConfig.setStarTreeIndexSpec(starTreeIndexSpec);

            final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
            driver.init(genConfig);
            driver.build();

            // Tar segment
            String segmentName = outputDir.list()[0];
            final String tarGzPath = TarGzCompressionUtils.createTarGzOfDirectory(outputDir.getAbsolutePath() + "/" +
                    segmentName, new File(segmentTarDir, segmentName).getAbsolutePath());
            LOGGER.info("Completed segment " + segmentNumber + " : " + segmentName +" from file " + inputAvroFile.getName());
            return new ImmutablePair<File, File>(inputAvroFile, new File(tarGzPath));
          } catch (Exception e) {
                LOGGER.error("Exception while building segment input: {} output {} ",
                    avroFiles.get(segmentIndex), "segment-" + segmentNumber);
                throw new RuntimeException(e);
          }
        }
      });

      futureTasks.add(buildSegmentFutureTask);
      executor.execute(buildSegmentFutureTask);
    }

    ListenableFuture<List<Pair<File, File>>> pairListFuture = Futures.allAsList(futureTasks);
    return Futures.transform(pairListFuture, new AsyncFunction<List<Pair<File, File>>, Map<File, File>>() {
      @Override
      public ListenableFuture<Map<File, File>> apply(List<Pair<File, File>> input) throws Exception {
        Map<File, File> avroToSegmentMap = new HashMap<File, File>();
        for (Pair<File, File> avroToSegmentPair : input) {
          avroToSegmentMap.put(avroToSegmentPair.getLeft(), avroToSegmentPair.getRight());
        }
        return Futures.immediateFuture(avroToSegmentMap);
      }
    });
  }

  protected void waitForRecordCountToStabilizeToExpectedCount(int expectedRecordCount, long deadlineMs) throws Exception {
    int pinotRecordCount = -1;
    final long startTimeMs = System.currentTimeMillis();

    do {
      Thread.sleep(5000L);

      try {
        // Run the query
        JSONObject response = postQuery("select count(*) from 'mytable'");
        JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
        JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
        String pinotValue = firstAggregationResult.getString("value");
        pinotRecordCount = Integer.parseInt(pinotValue);

        LOGGER.info("Pinot record count: " + pinotRecordCount + "\tExpected count: " + expectedRecordCount);
        TOTAL_DOCS = response.getLong("totalDocs");
      } catch (Exception e) {
        LOGGER.warn("Caught exception while waiting for record count to stabilize, will try again.", e);
      }

      if (expectedRecordCount > pinotRecordCount) {
        final long now = System.currentTimeMillis();
        if (now > deadlineMs) {
          Assert.fail("Failed to read " + expectedRecordCount + " records within the deadline (deadline=" + deadlineMs + "ms,now="
                  + now + "ms,NumRecordsRead=" + pinotRecordCount + ")");
        }
      }
    } while (pinotRecordCount < expectedRecordCount);

    if (expectedRecordCount != pinotRecordCount) {
      LOGGER.error("Got more records than expected");
      Assert.fail("Expecting " + expectedRecordCount + " but got " + pinotRecordCount);
    }
  }

  protected CountDownLatch setupSegmentCountCountDownLatch(final String tableName, final int expectedSegmentCount)
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    HelixManager manager =
        HelixManagerFactory
            .getZKHelixManager(getHelixClusterName(), "test_instance", InstanceType.SPECTATOR, ZkStarter.DEFAULT_ZK_STR);
    manager.connect();
    manager.addExternalViewChangeListener(new ExternalViewChangeListener() {
      private boolean _hasBeenTriggered = false;

      @Override
      public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        // Nothing to do?
        if (_hasBeenTriggered) {
          return;
        }

        for (ExternalView externalView : externalViewList) {
          if (externalView.getId().contains(tableName)) {

            Set<String> partitionSet = externalView.getPartitionSet();
            if (partitionSet.size() == expectedSegmentCount) {
              int onlinePartitionCount = 0;

              for (String partitionId : partitionSet) {
                Map<String, String> partitionStateMap = externalView.getStateMap(partitionId);
                if (partitionStateMap.containsValue("ONLINE")) {
                  onlinePartitionCount++;
                }
              }

              if (onlinePartitionCount == expectedSegmentCount) {
//                System.out.println("Got " + expectedSegmentCount + " online tables, unlatching the main thread");
                latch.countDown();
                _hasBeenTriggered = true;
              }
            }
          }
        }
      }
    });
    return latch;
  }

  public static void ensureDirectoryExistsAndIsEmpty(File tmpDir)
      throws IOException {
    FileUtils.deleteDirectory(tmpDir);
    tmpDir.mkdirs();
  }

  public static List<File> unpackAvroData(File tmpDir, int segmentCount)
      throws IOException, ArchiveException {
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(
            RealtimeClusterIntegrationTest.class.getClassLoader()
                .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), tmpDir);

    tmpDir.mkdirs();
    final List<File> avroFiles = new ArrayList<File>(segmentCount);
    for (int segmentNumber = 1; segmentNumber <= segmentCount; ++segmentNumber) {
      avroFiles.add(new File(tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }
    return avroFiles;
  }

  public void setupH2AndInsertAvro(final List<File> avroFiles, ExecutorService executor)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    _connection = DriverManager.getConnection("jdbc:h2:mem:");
    executor.execute(new Runnable() {
      @Override
      public void run() {
        createH2SchemaAndInsertAvroFiles(avroFiles, _connection);
      }
    });
  }

  public void setupQueryGenerator(final List<File> avroFiles, ExecutorService executor) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        _queryGenerator = new QueryGenerator(avroFiles, "mytable", "mytable");
      }
    });
  }

  public void pushAvroIntoKafka(final List<File> avroFiles, ExecutorService executor, final String kafkaTopic, final byte[] header) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        pushAvroIntoKafka(avroFiles, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, kafkaTopic, header);
      }
    });
  }

  public void pushAvroIntoKafka(final List<File> avroFiles, ExecutorService executor, final String kafkaTopic) {
    pushAvroIntoKafka(avroFiles, executor, kafkaTopic, null);
  }

  public File getSchemaFile() {
    return new File(OfflineClusterIntegrationTest.class.getClassLoader()
        .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema").getFile());
  }

  /**
   * NOTE:
   * <p>
   * For queries with <code>LIMIT</code> or <code>TOP</code>, need to remove limit or add <code>LIMIT 10000</code> to
   * the H2 SQL query because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code></li>
   * </ul>
   * <p>
   * For queries with multiple aggregation functions, need to split each of them into a separate H2 SQL query.
   * <ul>
   *   <li>Eg. <code>SELECT SUM(a), MAX(b) FROM table -> [SELECT SUM(a) FROM table, SELECT MAX(b) FROM table]</code></li>
   * </ul>
   * <p>
   * For group-by queries, need to add group-by columns to the select clause for H2 SQL query.
   * <ul>
   *   <li>Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code></li>
   * </ul>
   *
   * @throws Exception
   */
  @Test(enabled = false)
  public void testHardcodedQueries()
      throws Exception {
    // Here are some sample queries.
    String query;
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    runQuery(query, Arrays.asList("SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312",
        "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312"));
  }

  @Test
  public void testHardcodedQuerySet()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader()
        .getResource("On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K");
    Assert.assertNotNull(resourceUrl);
    File queriesFile = new File(resourceUrl.getFile());
    Random random = new Random();

    try (BufferedReader reader = new BufferedReader(new FileReader(queriesFile))) {
      while (true) {
        // Skip up to MAX_NUM_QUERIES_SKIPPED queries.
        int numQueriesSkipped = random.nextInt(MAX_NUM_QUERIES_SKIPPED);
        for (int i = 0; i < numQueriesSkipped; i++) {
          reader.readLine();
        }

        String queryString = reader.readLine();
        // Reach end of file.
        if (queryString == null) {
          return;
        }

        JSONObject query = new JSONObject(queryString);
        String pqlQuery = query.getString("pql");
        JSONArray hsqls = query.getJSONArray("hsqls");
        List<String> sqlQueries = new ArrayList<>();
        int length = hsqls.length();
        for (int i = 0; i < length; i++) {
          sqlQueries.add(hsqls.getString(i));
        }
        runQuery(pqlQuery, sqlQueries);
      }
    }
  }

  // This is disabled because testGeneratedQueriesWithMultiValues covers the same thing.
  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    testGeneratedQueries(false);
  }

  @Test
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    testGeneratedQueries(true);
  }

  protected void testGeneratedQueries(boolean withMultiValues)
      throws Exception {
    _queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    int generatedQueryCount = getGeneratedQueryCount();
    for (int i = 0; i < generatedQueryCount; i++) {
      QueryGenerator.Query query = _queryGenerator.generateQuery();
      String pqlQuery = query.generatePql();
      LOGGER.debug("Running query: {}", pqlQuery);
      runQuery(pqlQuery, query.generateH2Sql());
    }
  }

  protected String getSingleStringValueFromJSONAggregationResults(JSONObject jsonObject) throws JSONException {
    return jsonObject.getJSONArray("aggregationResults").getJSONObject(0).getString("value");
  }

  protected JSONArray getGroupByArrayFromJSONAggregationResults(JSONObject jsonObject) throws JSONException {
    return jsonObject.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
  }

  protected void ensurePinotConnectionIsCreated() {
    if (_pinotConnection == null) {
      synchronized (BaseClusterIntegrationTest.class) {
        if (_pinotConnection == null) {
          _pinotConnection = ConnectionFactory.fromZookeeper(ZkStarter.DEFAULT_ZK_STR + "/" + getHelixClusterName());
        }
      }
    }
  }

  protected long getCurrentServingNumDocs(String tableName) {
    ensurePinotConnectionIsCreated();
    try {
      com.linkedin.pinot.client.ResultSetGroup resultSetGroup =
          _pinotConnection.execute("SELECT COUNT(*) from " + tableName + " LIMIT 0");
      if (resultSetGroup.getResultSetCount() > 0) {
        return resultSetGroup.getResultSet(0).getInt(0);
      }
      return 0;
    } catch (Exception e) {
      return -1L;
    }
  }

  protected int getGeneratedQueryCount() {
    String generatedQueryCountProperty = System.getProperty("integration.test.generatedQueryCount");
    if (generatedQueryCountProperty != null) {
      return Integer.parseInt(generatedQueryCountProperty);
    } else {
      return GENERATED_QUERY_COUNT;
    }
  }
}
