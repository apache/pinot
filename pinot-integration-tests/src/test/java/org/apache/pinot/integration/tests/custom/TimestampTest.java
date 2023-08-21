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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TimestampTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "TimestampTest";
  private static final String TIMESTAMP_1 = "ts1";
  private static final String TIMESTAMP_2 = "ts2";
  private static final String LONG_1 = "long1";
  private static final String LONG_2 = "long2";

  private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @BeforeClass
  public void setUpTimeZone() {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
  }

  @AfterClass
  public void removeTimeZone() {
    TimeZone.setDefault(DEFAULT_TIME_ZONE);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT ts1, ts2, long1,long2 FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    long expectedTs1 = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
    long expectedTs2 = DateTimeFunctions.fromDateTime("2019-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss");

    for (int i = 0; i < getCountStarResult(); i++) {
      String ts1 = jsonNode.get("resultTable").get("rows").get(i).get(0).asText();
      String ts2 = jsonNode.get("resultTable").get("rows").get(i).get(1).asText();
      long long1 = jsonNode.get("resultTable").get("rows").get(i).get(2).asLong();
      long long2 = jsonNode.get("resultTable").get("rows").get(i).get(3).asLong();
      assertEquals(ts1, new Timestamp(expectedTs1).toString());
      assertEquals(ts2, new Timestamp(expectedTs2).toString());
      assertEquals(long1, expectedTs1);
      assertEquals(long2, expectedTs2);
      expectedTs1 += 86400000;
      expectedTs2 += 86400000;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectWithCastQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(ts1 AS TIMESTAMP) AS "
        + "VARCHAR), 'yyyy-MM-dd HH:mm:ss.S'), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS TIMESTAMP), 'MILLISECONDS') AS "
        + "TIMESTAMP) AS tdy_Calculation_2683863928708153344_ok\n"
        + "FROM %s\n"
        + "GROUP BY tdy_Calculation_2683863928708153344_ok\n"
        + "ORDER BY tdy_Calculation_2683863928708153344_ok ASC\n"
        + "LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    long expectedTs1 = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
    for (int i = 0; i < getCountStarResult(); i++) {
      String ts1 = jsonNode.get("resultTable").get("rows").get(i).get(0).asText();
      assertEquals(ts1, new Timestamp(expectedTs1).toString());
      expectedTs1 += 86400000;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectWithCastAndFilterQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(ts1 AS TIMESTAMP) AS "
        + "VARCHAR), 'yyyy-MM-dd HH:mm:ss.S'), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS TIMESTAMP), 'MILLISECONDS') AS "
        + "TIMESTAMP) AS tdy_Calculation_2683863928708153344_ok\n"
        + "FROM %s\n"
        + "WHERE   CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(ts1 AS TIMESTAMP) AS "
        + "VARCHAR), 'yyyy-MM-dd HH:mm:ss.S'), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS TIMESTAMP), 'MILLISECONDS') AS "
        + "TIMESTAMP) = FROMDATETIME( '2019-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')\n", getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").size(), 1);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText(), "2019-01-01 00:00:00.0");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimeExtractFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT HOUR(ts1), HOUR(ts2),\n"
        + "MINUTE(ts1), MINUTE(ts2),\n"
        + "SECOND(ts1), SECOND(ts2),\n"
        + "MILLISECOND(ts1), MILLISECOND(ts2),\n"
        + "YEAR(ts1), YEAR(ts2),\n"
        + "YEAR_OF_WEEK(ts1), YEAR_OF_WEEK(ts2),\n"
        + "MONTH_OF_YEAR(ts1), MONTH_OF_YEAR(ts2),\n"
        + "WEEK_OF_YEAR(ts1), WEEK_OF_YEAR(ts2),\n"
        + "DAY_OF_YEAR(ts1), DAY_OF_YEAR(ts2),\n"
        + "DAY_OF_MONTH(ts1), DAY_OF_MONTH(ts2),\n"
        + "DAY_OF_WEEK(ts1), DAY_OF_WEEK(ts2),\n"
        + "DOY(ts1), DOY(ts2),\n"
        + "DOW(ts1), DOW(ts2),\n"
        + "QUARTER(ts1), QUARTER(ts2)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(1).asInt(), 12);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(2).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(3).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(4).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(5).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(6).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(7).asInt(), 0);
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(8).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(9).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(10).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(11).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(12).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(13).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(14).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(15).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(16).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(17).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(18).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(19).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(20).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(21).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(22).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(23).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(24).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(25).asInt());
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(26).asInt(),
          jsonNode.get("resultTable").get("rows").get(i).get(27).asInt());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimestampDiffQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT TIMESTAMPDIFF(second, ts1, ts2)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 43200);
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPDIFF(minute, ts1, ts2)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 720);
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPDIFF(hour, ts1, ts2)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 12);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimestampAddQueriesMonth(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n" + "SELECT timestampAdd(MONTH, 2, ts1), ts1\n"
            + "FROM %s\n"
            + "LIMIT %d\n",
        getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    for (int i = 0; i < getCountStarResult(); i++) {
      LocalDateTime actual;
      LocalDateTime expected;
      if (useMultiStageQueryEngine) {
        String actualStr = jsonNode.get("resultTable").get("rows").get(i).get(0).asText();
        actual = simpleDateFormat.parse(actualStr).toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
        String expectedStr = jsonNode.get("resultTable").get("rows").get(i).get(1).asText();
        expected = simpleDateFormat.parse(expectedStr).toInstant()
            .atZone(ZoneOffset.UTC)
            .toLocalDateTime()
            .plusMonths(2);
      } else {
        long actualMillis = jsonNode.get("resultTable").get("rows").get(i).get(0).asLong();
        actual = Instant.ofEpochMilli(actualMillis)
            .atZone(ZoneOffset.UTC)
            .toLocalDateTime();
        String expectedStr = jsonNode.get("resultTable").get("rows").get(i).get(1).textValue();
        expected = simpleDateFormat.parse(expectedStr).toInstant()
            .atZone(ZoneOffset.UTC)
            .toLocalDateTime()
            .plusMonths(2);
      }
      Assert.assertEquals(actual, expected);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimestampAddQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT TIMESTAMPADD(MINUTE, 720, ts1), ts2\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      if (useMultiStageQueryEngine) {
        assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asText(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      } else {
        assertEquals(new Timestamp(jsonNode.get("resultTable").get("rows").get(i).get(0).longValue()).toString(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      }
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPADD(SECOND, 43200, ts1), ts2\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      if (useMultiStageQueryEngine) {
        assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asText(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      } else {
        assertEquals(new Timestamp(jsonNode.get("resultTable").get("rows").get(i).get(0).longValue()).toString(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      }
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPADD(HOUR, 12, ts1), ts2\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      if (useMultiStageQueryEngine) {
        assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asText(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      } else {
        assertEquals(new Timestamp(jsonNode.get("resultTable").get("rows").get(i).get(0).longValue()).toString(),
            jsonNode.get("resultTable").get("rows").get(i).get(1).textValue());
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testToDateTimeQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT "
        + (
        useMultiStageQueryEngine
            ? "TODATETIME(CAST(MIN(ts1) AS BIGINT), 'yyyy-MM-dd HH:mm:ss'),\n"
            : "TODATETIME(MIN(ts1), 'yyyy-MM-dd HH:mm:ss'),\n")
        + (
        useMultiStageQueryEngine
            ? "TODATETIME(CAST(MIN(ts2) AS BIGINT), 'yyyy-MM-dd HH:mm:ss')\n"
            : "TODATETIME(MIN(ts2), 'yyyy-MM-dd HH:mm:ss')\n")
        + "FROM %s\n", getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText(), "2019-01-01 00:00:00");
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).textValue(), "2019-01-01 12:00:00");
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(TIMESTAMP_1, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_2, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(LONG_1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_2, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(TIMESTAMP_1, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_2, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(LONG_1, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null,
            null),
        new org.apache.avro.Schema.Field(LONG_2, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null,
            null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      long ts1 = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
      long ts2 = DateTimeFunctions.fromDateTime("2019-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss");

      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(TIMESTAMP_1, ts1);
        record.put(TIMESTAMP_2, ts2);
        record.put(LONG_1, ts1);
        record.put(LONG_2, ts2);
        // add avro record to file
        fileWriter.append(record);
        ts1 += 86400000;
        ts2 += 86400000;
      }
    }
    return avroFile;
  }
}
