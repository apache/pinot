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

import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public abstract class BaseClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClusterIntegrationTest.class);

  protected Connection _connection;
  protected QueryGenerator _queryGenerator;

  protected abstract int getGeneratedQueryCount();

  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    try {
      Statement statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      // Run the query
      JSONObject response = postQuery(pqlQuery);
      JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
      JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
      if (firstAggregationResult.has("value")) {
        statement.execute(sqlQueries.get(0));
        ResultSet rs = statement.getResultSet();
        // Single value result for the aggregation, compare with the actual value
        String value = firstAggregationResult.getString("value");

        rs.first();
        String sqlValue = rs.getString(1);

        if (value != null && sqlValue != null) {
          // Strip decimals
          value = value.replaceAll("\\..*", "");
          sqlValue = sqlValue.replaceAll("\\..*", "");

          Assert.assertEquals(value, sqlValue, "Values did not match for query " + pqlQuery);
        } else {
          Assert.assertEquals(value, sqlValue, "Values did not match for query " + pqlQuery);
        }
      } else if (firstAggregationResult.has("groupByResult")) {
        // Load values from the query result
        for (int aggregationGroupIndex = 0; aggregationGroupIndex < aggregationResultsArray.length(); aggregationGroupIndex++) {
          JSONArray groupByResults = aggregationResultsArray.getJSONObject(aggregationGroupIndex).getJSONArray("groupByResult");
          if (groupByResults.length() != 0) {
            int groupKeyCount = groupByResults.getJSONObject(0).getJSONArray("group").length();

            Map<String, String> actualValues = new HashMap<String, String>();
            for (int resultIndex = 0; resultIndex < groupByResults.length(); ++resultIndex) {
              JSONArray group = groupByResults.getJSONObject(resultIndex).getJSONArray("group");
              String pinotGroupKey = "";
              for (int groupKeyIndex = 0; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                pinotGroupKey += group.getString(groupKeyIndex) + "\t";
              }

              actualValues.put(pinotGroupKey, Integer.toString((int) Double.parseDouble(groupByResults.getJSONObject(resultIndex).getString("value"))));
            }

            // Grouped result, build correct values and iterate through to compare both
            Map<String, String> correctValues = new HashMap<String, String>();
            statement.execute(sqlQueries.get(aggregationGroupIndex));
            ResultSet rs = statement.getResultSet();
            rs.beforeFirst();
            while (rs.next()) {
              String h2GroupKey = "";
              for (int groupKeyIndex = 0; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                h2GroupKey += rs.getString(groupKeyIndex + 1) + "\t";
              }
              correctValues.put(h2GroupKey, rs.getString(groupKeyCount + 1));
            }

            Assert.assertEquals(actualValues, correctValues, "Values did not match while running query " + pqlQuery);
          } else {
            // No records in group by, check that the result set is empty
            statement.execute(sqlQueries.get(aggregationGroupIndex));
            ResultSet rs = statement.getResultSet();
            Assert.assertTrue(rs.isLast(), "Pinot did not return any results while results were expected for query " + pqlQuery);
          }

        }
      }
    } catch (JSONException exception) {
      Assert.fail("Query did not return valid JSON while running query " + pqlQuery);
    }
    System.out.println();
  }

  public static void createH2SchemaAndInsertAvroFiles(List<File> avroFiles, Connection connection) {
    try {
      File schemaAvroFile = avroFiles.get(0);
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(schemaAvroFile, datumReader);

      Schema schema = dataFileReader.getSchema();
      List<Schema.Field> fields = schema.getFields();
      List<String> columnNamesAndTypes = new ArrayList<String>(fields.size());
      for (Schema.Field field : fields) {
        try {
          List<Schema> types = field.schema().getTypes();
          String columnNameAndType;
          if (types.size() == 1) {
            columnNameAndType = field.name() + " " + types.get(0).getName() + " not null";
          } else {
            columnNameAndType = field.name() + " " + types.get(0).getName();
          }

          columnNamesAndTypes.add(columnNameAndType.replace("string", "varchar(128)"));
        } catch (Exception e) {
          // Happens if the field is not a union, skip the field
        }
      }

      connection.prepareCall("create table mytable(" + StringUtil.join(",", columnNamesAndTypes.toArray(new String[columnNamesAndTypes.size()])) + ")").execute();
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
          for (int i = 0; i < columnNamesAndTypes.size(); i++) {
            Object value = record.get(i);
            if (value instanceof Utf8) {
              value = value.toString();
            }
            statement.setObject(i + 1, value);
          }
          statement.execute();
        }
        dataFileReader.close();
      }
      System.out.println("Insertion took " + (System.currentTimeMillis() - start));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void pushAvroIntoKafka(List<File> avroFiles, String kafkaBroker, String kafkaTopic) {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBroker);
    properties.put("serializer.class", "kafka.serializer.NullEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<String, byte[]> producer = new Producer<String, byte[]>(producerConfig);
    for (File avroFile : avroFiles) {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536);
        DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(reader.getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(reader.getSchema(), outputStream);
        for (GenericRecord genericRecord : reader) {
          outputStream.reset();
          dataFileWriter.append(genericRecord);

          KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(kafkaTopic, outputStream.toByteArray());
          producer.send(data);
        }
        outputStream.close();
        reader.close();
        LOGGER.info("Finished writing " + avroFile.getName() + " into Kafka topic " + kafkaTopic);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

  }

  public static void buildSegmentsFromAvro(final List<File> avroFiles, Executor executor, int baseSegmentIndex,
      final File baseDirectory) {
    int segmentCount = avroFiles.size();
    System.out.println("Building " + segmentCount + " segments in parallel");
    for(int i = 1; i <= segmentCount; ++i) {
      final int segmentIndex = i - 1;
      final int segmentNumber = i + baseSegmentIndex;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // Build segment
            System.out.println("Starting to build segment " + segmentNumber);
            File outputDir = new File(baseDirectory, "segment-" + segmentNumber);
            final SegmentGeneratorConfig genConfig =
                SegmentTestUtils
                    .getSegmentGenSpecWithSchemAndProjectedColumns(avroFiles.get(segmentIndex), outputDir,
                        "daysSinceEpoch", TimeUnit.DAYS, "myresource", "mytable");

            genConfig.setSegmentNamePostfix(Integer.toString(segmentNumber));

            final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
            driver.init(genConfig);
            driver.build();

            // Tar segment
            TarGzCompressionUtils
                .createTarGzOfDirectory(outputDir.getAbsolutePath() + "/myresource_mytable_" + segmentNumber,
                    new File(outputDir.getParent(), "myresource_mytable_" + segmentNumber).getAbsolutePath());

            System.out.println("Completed segment " + segmentNumber);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
  }

  @Test
  public void testMultipleQueries() throws Exception {
    QueryGenerator.Query[] queries = new QueryGenerator.Query[getGeneratedQueryCount()];
    for (int i = 0; i < queries.length; i++) {
      queries[i] = _queryGenerator.generateQuery();
    }

    for (QueryGenerator.Query query : queries) {
      System.out.println(query.generatePql());

      runQuery(query.generatePql(), query.generateH2Sql());
    }
  }

  @Test
  public void testHardcodedQuerySet() throws Exception {
    String[] queries = new String[] {
        "select count(*) from 'myresource.mytable'",
        "select sum(DepDelay) from 'myresource.mytable'",
        "select count(DepDelay) from 'myresource.mytable'",
        "select min(DepDelay) from 'myresource.mytable'",
        "select max(DepDelay) from 'myresource.mytable'",
        "select avg(DepDelay) from 'myresource.mytable'",
        "select Carrier, count(*) from 'myresource.mytable' group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where ArrDelay > 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where Cancelled = 1 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay >= 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay < 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where ArrDelay <= 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay >= 15 or ArrDelay >= 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay < 15 and ArrDelay <= 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay between 5 and 15 group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay in (2, 8, 42) group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where DepDelay not in (4, 16) group by Carrier",
        "select Carrier, count(*) from 'myresource.mytable' where Cancelled <> 1 group by Carrier",
        "select Carrier, min(ArrDelay) from 'myresource.mytable' group by Carrier",
        "select Carrier, max(ArrDelay) from 'myresource.mytable' group by Carrier",
        "select Carrier, sum(ArrDelay) from 'myresource.mytable' group by Carrier",
        "select TailNum, avg(ArrDelay) from 'myresource.mytable' group by TailNum",
        "select FlightNum, avg(ArrDelay) from 'myresource.mytable' group by FlightNum",
        "select distinct Carrier from 'myresource.mytable' where TailNum = 'D942DN'"
    };

    for (String query : queries) {
      System.out.println(query);
      runQuery(query, Collections.singletonList(query.replace("'myresource.mytable'", "mytable")));
    }
  }
}
