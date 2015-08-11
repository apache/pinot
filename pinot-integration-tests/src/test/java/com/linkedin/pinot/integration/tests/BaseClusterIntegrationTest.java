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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;


/**
 * TODO Document me!
 *
 */
public abstract class BaseClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClusterIntegrationTest.class);
  private static final AtomicInteger totalAvroRecordWrittenCount = new AtomicInteger(0);
  private static final boolean BATCH_KAFKA_MESSAGES = true;
  private static final int MAX_MULTIVALUE_CARDINALITY = 5;
  protected static final boolean GATHER_FAILED_QUERIES = false;
  private int failedQueryCount = 0;
  private int queryCount = 0;

  protected Connection _connection;
  protected QueryGenerator _queryGenerator;
  protected static long TOTAL_DOCS = 115545;

  protected File queriesFile;

  protected com.linkedin.pinot.client.Connection _pinotConnection = null;

  private class NullableStringComparator implements Comparator<String> {
    @Override
    public int compare(String left, String right) {
      if (left == null) {
        if (right == null) {
          return 0;
        } else {
          return -1;
        }
      } else {
        if (right == null) {
          return 1;
        } else {
          return left.compareTo(right);
        }
      }
    }
  }

  @BeforeMethod
  public void resetQueryCounts() {
    failedQueryCount = 0;
    queryCount = 0;
  }

  @AfterMethod
  public void checkFailedQueryCount() {
    if (GATHER_FAILED_QUERIES && failedQueryCount != 0) {
      File file = new File(getClass().getSimpleName() + "-failed.txt");
      PrintWriter out = null;
      try {
        out = new PrintWriter(new FileWriter(file, true));
        out.println("# " + failedQueryCount + "/" + queryCount + " queries did not match with H2");
        out.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to write to failed queries file", e);
      } finally {
        IOUtils.closeQuietly(out);
      }
    }

    Assert.assertTrue(failedQueryCount == 0, "Queries have failed during this test.");
  }

  protected void runNoH2ComparisonQuery(String pqlQuery) throws Exception {
    JSONObject ret = postQuery(pqlQuery);
    ret.put("pql", pqlQuery);
    try {
      Assert.assertEquals(ret.getJSONArray("exceptions").length(), 0);
      Assert.assertEquals(ret.getLong("totalDocs"), TOTAL_DOCS);
    } catch (AssertionError e) {
      LOGGER.error("********** pql: {}, result: {}", ret.toString(1), e);
      throw e;
    }
  }

  private void saveFailedQuery(String pqlQuery, List<String> sqlQueries, String... messages) {
    failedQueryCount++;

    File file = new File(getClass().getSimpleName() + "-failed.txt");
    PrintWriter out = null;
    try {
      out = new PrintWriter(new FileWriter(file, true));
      for (String message : messages) {
        out.print("# ");
        out.println(message);
        LOGGER.warn(message);
      }
      out.print(pqlQuery);
      out.print("\t");
      out.println(StringUtil.join("\t", sqlQueries.toArray(new String[sqlQueries.size()])));
      out.println();
      out.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to write to failed queries file", e);
    } finally {
      IOUtils.closeQuietly(out);
    }
  }

  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    try {
      // TODO Use Pinot client API for this
      queryCount++;
      Statement statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      // Run the query
      JSONObject response = postQuery(pqlQuery);
      if (response.getJSONArray("exceptions").length() > 0) {
        String processingException = (String) response.getJSONArray("exceptions").get(0);
        // Ignore all the PQL parsing error.
        if (processingException.contains("ProcessingException(errorCode:350,")) {
          return;
        }
        if (GATHER_FAILED_QUERIES) {
          saveFailedQuery(pqlQuery, sqlQueries, "Got exceptions in pql query " + pqlQuery + ", got " + response);
        } else {
          Assert.fail("Got exceptions in pql query: " + pqlQuery);
        }
      }

      if (response.has("aggregationResults") && response.getJSONArray("aggregationResults").length() != 0) {
        JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
        JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
        if (firstAggregationResult.has("value")) {
          LOGGER.debug("Trying to execute sql query: " + sqlQueries.get(0));
          statement.execute(sqlQueries.get(0));
          ResultSet rs = statement.getResultSet();
          LOGGER.debug("Trying to get result from sql: " + rs);
          // Single value result for the aggregation, compare with the actual value
          String bqlValue = firstAggregationResult.getString("value");

          rs.first();
          String sqlValue = rs.getString(1);

          if (bqlValue != null && sqlValue != null) {
            // Strip decimals
            bqlValue = bqlValue.replaceAll("\\..*", "");
            sqlValue = sqlValue.replaceAll("\\..*", "");
          }

          LOGGER.debug("bql value: " + bqlValue);
          LOGGER.debug("sql value: " + sqlValue);
          if (GATHER_FAILED_QUERIES) {
            if (!EqualityUtils.isEqual(bqlValue, sqlValue)) {
              saveFailedQuery(pqlQuery, sqlQueries, "Values did not match for query " + pqlQuery + ", expected "
                  + sqlValue + ", got " + bqlValue);
            }
          } else {
            Assert.assertEquals(bqlValue, sqlValue, "Values did not match for query " + pqlQuery);
          }
        } else if (firstAggregationResult.has("groupByResult")) {
          // Load values from the query result
          for (int aggregationGroupIndex = 0; aggregationGroupIndex < aggregationResultsArray.length(); aggregationGroupIndex++) {
            JSONArray groupByResults =
                aggregationResultsArray.getJSONObject(aggregationGroupIndex).getJSONArray("groupByResult");
            if (groupByResults.length() != 0) {
              int groupKeyCount = groupByResults.getJSONObject(0).getJSONArray("group").length();

              Map<String, String> actualValues = new TreeMap<String, String>(new NullableStringComparator());
              for (int resultIndex = 0; resultIndex < groupByResults.length(); ++resultIndex) {
                JSONArray group = groupByResults.getJSONObject(resultIndex).getJSONArray("group");
                String pinotGroupKey = group.getString(0);
                for (int groupKeyIndex = 1; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                  pinotGroupKey += "\t" + group.getString(groupKeyIndex);
                }

                actualValues.put(pinotGroupKey, groupByResults.getJSONObject(resultIndex).getString("value"));
              }

              // Grouped result, build correct values and iterate through to compare both
              Map<String, String> correctValues = new TreeMap<String, String>(new NullableStringComparator());
              LOGGER.info("Trying to execute sql query: " + sqlQueries.get(aggregationGroupIndex));
              statement.execute(sqlQueries.get(aggregationGroupIndex));
              ResultSet rs = statement.getResultSet();
              LOGGER.debug("Trying to get result from sql: " + rs);
              rs.beforeFirst();
              try {
                while (rs.next()) {
                  String h2GroupKey = rs.getString(1);
                  for (int groupKeyIndex = 1; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                    h2GroupKey += "\t" + rs.getString(groupKeyIndex + 1);
                  }
                  correctValues.put(h2GroupKey, rs.getString(groupKeyCount + 1));
                }
              } catch (Exception e) {
                LOGGER.error("Catch exception when constructing H2 results for group by query", e);
              }
              LOGGER.debug("Trying to get result from sql: " + correctValues.toString());
              LOGGER.debug("Trying to compare result from bql: " + actualValues);
              LOGGER.debug("Trying to compare result from sql: " + correctValues);

              if (correctValues.size() < 10000) {
                // Check that Pinot results are contained in the SQL results
                Set<String> pinotKeys = actualValues.keySet();
                for (String pinotKey : pinotKeys) {
                  if (GATHER_FAILED_QUERIES) {
                    if (!correctValues.containsKey((pinotKey))) {
                      saveFailedQuery(pqlQuery, sqlQueries, "Result group '" + pinotKey
                          + "' returned by Pinot was not returned by H2 for query " + pqlQuery, "Bql values: "
                          + actualValues, "Sql values: " + correctValues);
                      break;
                    } else {
                      double actualValue = Double.parseDouble(actualValues.get(pinotKey));
                      double correctValue = Double.parseDouble(correctValues.get(pinotKey));
                      if (1.0 < Math.abs(actualValue - correctValue)) {
                        saveFailedQuery(pqlQuery, sqlQueries, "Results differ between Pinot and H2 for query "
                            + pqlQuery + ", expected " + correctValue + ", got " + actualValue + " for group "
                            + pinotKey, "Bql values: " + actualValues, "Sql values: " + correctValues);
                        break;
                      }
                    }
                  } else {
                    Assert.assertTrue(correctValues.containsKey(pinotKey), "Result group '" + pinotKey
                        + "' returned by Pinot was not returned by H2 for query " + pqlQuery);
                    Assert.assertEquals(
                        Double.parseDouble(actualValues.get(pinotKey)),
                        Double.parseDouble(correctValues.get(pinotKey)),
                        1.0d,
                        "Results differ between Pinot and H2 for query " + pqlQuery + ", expected "
                            + correctValues.get(pinotKey) + ", got " + actualValues.get(pinotKey) + " for group "
                            + pinotKey + "\nBql values: " + actualValues + "\nSql values: " + correctValues);
                  }
                }
              } else {
                LOGGER.warn("SQL query returned more than 10k rows, skipping comparison");
                queryCount--;
              }
            } else {
              // No records in group by, check that the result set is empty
              statement.execute(sqlQueries.get(aggregationGroupIndex));
              ResultSet rs = statement.getResultSet();
              rs.beforeFirst();
              int rowCount = 0;
              while (rs.next()) {
                rowCount++;
              }

              if (rowCount != 0) {
                // Resultset not empty, while Pinot has no results
                if (GATHER_FAILED_QUERIES) {
                  saveFailedQuery(pqlQuery, sqlQueries, "Pinot did not return any results while " + rowCount
                      + " rows were expected for query " + pqlQuery);
                } else {
                  Assert.fail("Pinot did not return any results while " + rowCount
                      + " results were expected for query " + pqlQuery);
                }
              }
            }
          }
        }
      } else {
        // Don't compare selection results for now
      }
    } catch (JSONException exception) {
      if (GATHER_FAILED_QUERIES) {
        saveFailedQuery(pqlQuery, sqlQueries, "Query did not return valid JSON while running query " + pqlQuery);
        exception.printStackTrace();
      } else {
        Assert.fail("Query did not return valid JSON while running query " + pqlQuery, exception);
      }
    }
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
              columnNameAndType = field.name() + " " + typeName + " not null";
            } else {
              columnNameAndType = field.name() + " " + typeName;
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

            for (int i = 0; i < MAX_MULTIVALUE_CARDINALITY; i++) {
              columnNamesAndTypes.add(field.name() + i + " " + elementTypeName);
            }
            ++columnCount;
            break;
          default:
            throw new AssertionError("Don't know how to handle field " + fieldName + " of type " + fieldType);
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
              for (int i = 0; i < MAX_MULTIVALUE_CARDINALITY; i++) {
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
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<String, byte[]> producer = new Producer<String, byte[]>(producerConfig);
    for (File avroFile : avroFiles) {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536);
        DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
        BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(reader.getSchema());
        int recordCount = 0;
        List<KeyedMessage<String, byte[]>> messagesToWrite = new ArrayList<KeyedMessage<String, byte[]>>(10000);
        for (GenericRecord genericRecord : reader) {
          outputStream.reset();
          datumWriter.write(genericRecord, binaryEncoder);
          binaryEncoder.flush();

          byte[] bytes = outputStream.toByteArray();
          KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(kafkaTopic, bytes);

          if (BATCH_KAFKA_MESSAGES) {
            messagesToWrite.add(data);
          } else {
            producer.send(data);
          }
          recordCount += 1;
        }

        if (BATCH_KAFKA_MESSAGES) {
          producer.send(messagesToWrite);
        }

        outputStream.close();
        reader.close();
        LOGGER.info("Finished writing " + recordCount + " records from " + avroFile.getName() + " into Kafka topic "
            + kafkaTopic);
        int totalRecordCount = totalAvroRecordWrittenCount.addAndGet(recordCount);
        LOGGER.info("Total records written so far " + totalRecordCount);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public static void buildSegmentsFromAvro(final List<File> avroFiles, Executor executor, int baseSegmentIndex,
      final File baseDirectory, final File segmentTarDir, final String tableName) {
    int segmentCount = avroFiles.size();
    LOGGER.info("Building " + segmentCount + " segments in parallel");
    for (int i = 1; i <= segmentCount; ++i) {
      final int segmentIndex = i - 1;
      final int segmentNumber = i + baseSegmentIndex;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // Build segment
            LOGGER.info("Starting to build segment " + segmentNumber);
            File outputDir = new File(baseDirectory, "segment-" + segmentNumber);
            final SegmentGeneratorConfig genConfig =
                SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avroFiles.get(segmentIndex), outputDir,
                    TimeUnit.DAYS, tableName);

            genConfig.setSegmentNamePostfix(Integer.toString(segmentNumber));

            final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
            driver.init(genConfig);
            driver.build();

            // Tar segment
            String segmentName = outputDir.list()[0];
            TarGzCompressionUtils.createTarGzOfDirectory(outputDir.getAbsolutePath() + "/" + segmentName, new File(
                segmentTarDir, segmentName).getAbsolutePath());
            LOGGER.info("Completed segment " + segmentNumber + " : " + segmentName);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
  }

  @Test
  public void testMultipleQueries() throws Exception {
    queriesFile =
        new File(TestUtils.getFileFromResourceUrl(BaseClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K")));

    Scanner scanner = new Scanner(queriesFile);
    scanner.useDelimiter("\n");
    String[] pqls = new String[1000];

    for (int i = 0; i < pqls.length; i++) {
      JSONObject test_case = new JSONObject(scanner.next());
      pqls[i] = test_case.getString("pql");
    }

    for (String query : pqls) {
      try {
        runNoH2ComparisonQuery(query);
      } catch (Exception e) {
        LOGGER.error("Getting error query: {}" + query);
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  @Test
  public void testHardcodedQuerySet() throws Exception {
    for (String query : getHardCodedQuerySet()) {
      try {
        LOGGER.info("Trying to send query : {}", query);
        runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable")));
      } catch (Exception e) {
        LOGGER.error("Getting erro for query : {}", query);
      }

    }
  }

  @Test
  public void testGeneratedQueries() throws Exception {
    int generatedQueryCount = getGeneratedQueryCount();

    String generatedQueryCountProperty = System.getProperty("integration.test.generatedQueryCount");
    if (generatedQueryCountProperty != null) {
      generatedQueryCount = Integer.parseInt(generatedQueryCountProperty);
    }

    QueryGenerator.Query[] queries = new QueryGenerator.Query[generatedQueryCount];
    _queryGenerator.setSkipMultivaluePredicates(true);
    for (int i = 0; i < queries.length; i++) {
      queries[i] = _queryGenerator.generateQuery();
    }

    for (QueryGenerator.Query query : queries) {
      LOGGER.info("Trying to send query : {}", query.generatePql());
      runQuery(query.generatePql(), query.generateH2Sql());
    }
  }

  @Test
  public void testGeneratedQueriesWithMultivalues() throws Exception {
    int generatedQueryCount = getGeneratedQueryCount();

    String generatedQueryCountProperty = System.getProperty("integration.test.generatedQueryCount");
    if (generatedQueryCountProperty != null) {
      generatedQueryCount = Integer.parseInt(generatedQueryCountProperty);
    }

    QueryGenerator.Query[] queries = new QueryGenerator.Query[generatedQueryCount];
    _queryGenerator.setSkipMultivaluePredicates(false);
    for (int i = 0; i < queries.length; i++) {
      queries[i] = _queryGenerator.generateQuery();
    }

    for (QueryGenerator.Query query : queries) {
      LOGGER.info("Trying to send query : {}", query.generatePql());
      runQuery(query.generatePql(), query.generateH2Sql());
    }
  }

  protected String getSingleStringValueFromJSONAggregationResults(JSONObject jsonObject) throws JSONException {
    return jsonObject.getJSONArray("aggregationResults").getJSONObject(0).getString("value");
  }

  protected JSONArray getGroupByArrayFromJSONAggregationResults(JSONObject jsonObject) throws JSONException {
    return jsonObject.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
  }

  protected String[] getHardCodedQuerySet() {
    String[] queries =
        new String[] { "SELECT AirTime, avg(TotalAddGTime) FROM 'mytable'  WHERE DivAirportLandings BETWEEN 0 AND 0 OR Quarter IN (2, 2, 4, 2, 3, 1, 1, 1) GROUP BY AirTime LIMIT 10000", "SELECT count(*) FROM 'mytable'  WHERE DayofMonth IN ('19', '10', '28', '1', '25', '2') ", "SELECT count(*) FROM 'mytable'  WHERE TaxiOut IN ('35', '70', '29', '74', '126', '106', '70', '134', '118', '43') OR DayofMonth IN ('19', '10', '28', '1', '25') ", "SELECT ArrDelay, avg(DestCityMarketID) FROM 'mytable'  WHERE TaxiOut IN ('35', '70', '29', '74', '126', '106', '70', '134', '118', '43') OR DayofMonth IN ('19', '10', '28', '1', '25') GROUP BY ArrDelay LIMIT 10000", "SELECT OriginAirportSeqID, min(CRSArrTime) FROM 'mytable'  WHERE TaxiOut BETWEEN 140 AND 26 OR DestCityName >= 'Gainesville, FL' GROUP BY OriginAirportSeqID LIMIT 10000", "SELECT NASDelay, DestAirportSeqID, min(DayOfWeek) FROM 'mytable'  WHERE DaysSinceEpoch IN ('16426', '16176', '16314', '16321') GROUP BY NASDelay, DestAirportSeqID LIMIT 10000", "SELECT DestState, avg(DistanceGroup) FROM 'mytable'  GROUP BY DestState LIMIT 10000", "SELECT ActualElapsedTime, DestCityMarketID, sum(OriginAirportSeqID) FROM 'mytable'  WHERE DestStateName > 'Oklahoma' GROUP BY ActualElapsedTime, DestCityMarketID LIMIT 10000", "SELECT sum(CarrierDelay) FROM 'mytable'  WHERE      CRSDepTime < '1047' OR DestWac = '84'   LIMIT 16", "SELECT Year, sum(CarrierDelay) FROM 'mytable'  WHERE DestWac BETWEEN '84' AND '37' OR CRSDepTime < '1047' GROUP BY Year LIMIT 10000", "select count(*) from 'mytable'", "select sum(DepDelay) from 'mytable'", "select count(DepDelay) from 'mytable'", "select min(DepDelay) from 'mytable'", "select max(DepDelay) from 'mytable'", "select avg(DepDelay) from 'mytable'", "select Carrier, count(*) from 'mytable' group by Carrier  ", "select Carrier, count(*) from 'mytable' where ArrDelay > 15 group by Carrier  ", "select Carrier, count(*) from 'mytable' where Cancelled = 1 group by Carrier  ", "select Carrier, count(*) from 'mytable' where DepDelay >= 15 group by Carrier  ", "select Carrier, count(*) from 'mytable' where DepDelay < 15 group by Carrier ", "select Carrier, count(*) from 'mytable' where ArrDelay <= 15 group by Carrier  ", "select Carrier, count(*) from 'mytable' where DepDelay >= 15 or ArrDelay >= 15 group by Carrier  ", "select Carrier, count(*) from 'mytable' where DepDelay < 15 and ArrDelay <= 15 group by Carrier ", "select Carrier, count(*) from 'mytable' where DepDelay between 5 and 15 group by Carrier  ", "select Carrier, count(*) from 'mytable' where DepDelay in (2, 8, 42) group by Carrier ", "select Carrier, count(*) from 'mytable' where DepDelay not in (4, 16) group by Carrier ", "select Carrier, count(*) from 'mytable' where Cancelled <> 1 group by Carrier ", "select Carrier, min(ArrDelay) from 'mytable' group by Carrier ", "select Carrier, max(ArrDelay) from 'mytable' group by Carrier ", "select Carrier, sum(ArrDelay) from 'mytable' group by Carrier ", "select TailNum, avg(ArrDelay) from 'mytable' group by TailNum ", "select FlightNum, avg(ArrDelay) from 'mytable' group by FlightNum ", "select distinctCount(Carrier) from 'mytable' where TailNum = 'D942DN' ", "SELECT count(*) FROM 'mytable'  WHERE OriginStateName BETWEEN 'U.S. Pacific Trust Territories and Possessions' AND 'Maryland'  " };
    return queries;
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

  protected long getCurrentServingNumDocs() {
    ensurePinotConnectionIsCreated();
    com.linkedin.pinot.client.ResultSet resultSet =
        _pinotConnection.execute("SELECT COUNT(*) from mytable LIMIT 0").getResultSet(0);
    return resultSet.getInt(0);
  }

  protected int getGeneratedQueryCount() {
    return -1;
  }
}
