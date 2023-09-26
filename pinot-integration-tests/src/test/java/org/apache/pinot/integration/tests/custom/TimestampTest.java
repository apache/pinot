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
import java.util.TimeZone;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TimestampTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "TimestampTest";
  private static final String TIMESTAMP_BASE = "tsBase";
  private static final String TIMESTAMP_HALF_DAY_AFTER = "tsHalfDayAfter"; // 6 hours after TIMESTAMP_BASE
  private static final String TIMESTAMP_ONE_DAY_AFTER = "tsOneDayAfter"; // 1 day after TIMESTAMP_BASE
  private static final String TIMESTAMP_ONE_WEEK_AFTER = "tsOneWeekAfter"; // 1 week after TIMESTAMP_BASE
  private static final String TIMESTAMP_ONE_MONTH_AFTER = "tsOneMonthAfter"; // 1 month after TIMESTAMP_BASE
  private static final String TIMESTAMP_ONE_QUARTER_AFTER = "tsOneQuarterAfter"; // 1 quarter after TIMESTAMP_BASE
  private static final String TIMESTAMP_ONE_YEAR_AFTER = "tsOneYearAfter"; // 1 year after TIMESTAMP_BASE
  private static final String LONG_BASE = "longBase";
  private static final String LONG_HALF_DAY_AFTER = "longHalfDayAfter";
  private static final String LONG_ONE_DAY_AFTER = "longOneDayAfter";
  private static final String LONG_ONE_WEEK_AFTER = "longOneWeekAfter";
  private static final String LONG_ONE_MONTH_AFTER = "longOneMonthAfter";
  private static final String LONG_ONE_QUARTER_AFTER = "longOneQuarterAfter";
  private static final String LONG_ONE_YEAR_AFTER = "longOneYearAfter";

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
    String query =
        String.format("SELECT tsBase, tsHalfDayAfter, longBase,longHalfDayAfter FROM %s LIMIT %d", getTableName(),
            getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    long expectedTsBase = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
    long expectedTsHalfDayAfter = DateTimeFunctions.fromDateTime("2019-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss");

    for (int i = 0; i < getCountStarResult(); i++) {
      String tsBase = jsonNode.get("resultTable").get("rows").get(i).get(0).asText();
      String tsHalfDayAfter = jsonNode.get("resultTable").get("rows").get(i).get(1).asText();
      long longBase = jsonNode.get("resultTable").get("rows").get(i).get(2).asLong();
      long longHalfDayAfter = jsonNode.get("resultTable").get("rows").get(i).get(3).asLong();
      assertEquals(tsBase, new Timestamp(expectedTsBase).toString());
      assertEquals(tsHalfDayAfter, new Timestamp(expectedTsHalfDayAfter).toString());
      assertEquals(longBase, expectedTsBase);
      assertEquals(longHalfDayAfter, expectedTsHalfDayAfter);
      expectedTsBase += 86400000;
      expectedTsHalfDayAfter += 86400000;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectWithCastQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(tsBase AS TIMESTAMP) AS "
        + "VARCHAR), 'yyyy-MM-dd HH:mm:ss.S'), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS TIMESTAMP), 'MILLISECONDS') AS "
        + "TIMESTAMP) AS tdy_Calculation_2683863928708153344_ok\n"
        + "FROM %s\n"
        + "GROUP BY tdy_Calculation_2683863928708153344_ok\n"
        + "ORDER BY tdy_Calculation_2683863928708153344_ok ASC\n"
        + "LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    long expectedTsBase = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
    for (int i = 0; i < getCountStarResult(); i++) {
      String tsBase = jsonNode.get("resultTable").get("rows").get(i).get(0).asText();
      assertEquals(tsBase, new Timestamp(expectedTsBase).toString());
      expectedTsBase += 86400000;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectWithCastAndFilterQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("\n"
        + "SELECT CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(tsBase AS TIMESTAMP) AS "
        + "VARCHAR), 'yyyy-MM-dd HH:mm:ss.S'), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS TIMESTAMP), 'MILLISECONDS') AS "
        + "TIMESTAMP) AS tdy_Calculation_2683863928708153344_ok\n"
        + "FROM %s\n"
        + "WHERE   CAST(DATETRUNC('DAY', CAST(FROMDATETIME(TODATETIME(FROMDATETIME(CAST(CAST(tsBase AS TIMESTAMP) AS "
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
        + "SELECT HOUR(tsBase), HOUR(tsHalfDayAfter),\n"
        + "MINUTE(tsBase), MINUTE(tsHalfDayAfter),\n"
        + "SECOND(tsBase), SECOND(tsHalfDayAfter),\n"
        + "MILLISECOND(tsBase), MILLISECOND(tsHalfDayAfter),\n"
        + "YEAR(tsBase), YEAR(tsHalfDayAfter),\n"
        + "YEAR_OF_WEEK(tsBase), YEAR_OF_WEEK(tsHalfDayAfter),\n"
        + "MONTH_OF_YEAR(tsBase), MONTH_OF_YEAR(tsHalfDayAfter),\n"
        + "WEEK_OF_YEAR(tsBase), WEEK_OF_YEAR(tsHalfDayAfter),\n"
        + "DAY_OF_YEAR(tsBase), DAY_OF_YEAR(tsHalfDayAfter),\n"
        + "DAY_OF_MONTH(tsBase), DAY_OF_MONTH(tsHalfDayAfter),\n"
        + "DAY_OF_WEEK(tsBase), DAY_OF_WEEK(tsHalfDayAfter),\n"
        + "DOY(tsBase), DOY(tsHalfDayAfter),\n"
        + "DOW(tsBase), DOW(tsHalfDayAfter),\n"
        + "QUARTER(tsBase), QUARTER(tsHalfDayAfter)\n"
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
        + "SELECT TIMESTAMPDIFF(second, tsBase, tsHalfDayAfter)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 43200);
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPDIFF(minute, tsBase, tsHalfDayAfter)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 720);
    }

    query = String.format("\n"
        + "SELECT TIMESTAMPDIFF(hour, tsBase, tsHalfDayAfter)\n"
        + "FROM %s\n"
        + "LIMIT %d\n", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(jsonNode.get("resultTable").get("rows").get(i).get(0).asLong(), 12);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimestampAddQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query;
    JsonNode jsonNode;
    query = String.format("\n"
        + "SELECT TIMESTAMPADD(YEAR, 1, tsBase), tsOneYearAfter\n"
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
        + "SELECT TIMESTAMPADD(QUARTER, 1, tsBase), tsOneQuarterAfter\n"
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
        + "SELECT TIMESTAMPADD(MONTH, 1, tsBase), tsOneMonthAfter\n"
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
        + "SELECT TIMESTAMPADD(WEEK, 1, tsBase), tsOneWeekAfter\n"
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
        + "SELECT TIMESTAMPADD(DAY, 1, tsBase), tsOneDayAfter\n"
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
        + "SELECT TIMESTAMPADD(MINUTE, 720, tsBase), tsHalfDayAfter\n"
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
        + "SELECT TIMESTAMPADD(SECOND, 43200, tsBase), tsHalfDayAfter\n"
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
        + "SELECT TIMESTAMPADD(HOUR, 12, tsBase), tsHalfDayAfter\n"
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
            ? "TODATETIME(CAST(MIN(tsBase) AS BIGINT), 'yyyy-MM-dd HH:mm:ss'),\n"
            : "TODATETIME(MIN(tsBase), 'yyyy-MM-dd HH:mm:ss'),\n")
        + (
        useMultiStageQueryEngine
            ? "TODATETIME(CAST(MIN(tsHalfDayAfter) AS BIGINT), 'yyyy-MM-dd HH:mm:ss')\n"
            : "TODATETIME(MIN(tsHalfDayAfter), 'yyyy-MM-dd HH:mm:ss')\n")
        + "FROM %s\n", getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText(), "2019-01-01 00:00:00");
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).textValue(), "2019-01-01 12:00:00");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLastWithTimeQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT LASTWITHTIME(longBase, longBase, 'long'), LASTWITHTIME(longBase, tsBase, 'long') FROM %s\n",
        getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).longValue(), 1632614400000L);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).longValue(), 1632614400000L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFirstWithTimeQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT FIRSTWITHTIME(longBase, longBase, 'long'), FIRSTWITHTIME(longBase, tsBase, 'long') FROM %s\n",
        getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).longValue(), 1546300800000L);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).longValue(), 1546300800000L);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(TIMESTAMP_BASE, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_HALF_DAY_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_ONE_DAY_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_ONE_WEEK_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_ONE_MONTH_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_ONE_QUARTER_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(TIMESTAMP_ONE_YEAR_AFTER, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(LONG_BASE, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_HALF_DAY_AFTER, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_ONE_DAY_AFTER, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_ONE_WEEK_AFTER, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_ONE_MONTH_AFTER, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_ONE_QUARTER_AFTER, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_ONE_YEAR_AFTER, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new Field(TIMESTAMP_BASE, create(Type.LONG), null, null),
        new Field(TIMESTAMP_HALF_DAY_AFTER, create(Type.LONG), null, null),
        new Field(TIMESTAMP_ONE_DAY_AFTER, create(Type.LONG), null, null),
        new Field(TIMESTAMP_ONE_WEEK_AFTER, create(Type.LONG), null, null),
        new Field(TIMESTAMP_ONE_MONTH_AFTER, create(Type.LONG), null, null),
        new Field(TIMESTAMP_ONE_QUARTER_AFTER, create(Type.LONG), null, null),
        new Field(TIMESTAMP_ONE_YEAR_AFTER, create(Type.LONG), null, null),
        new Field(LONG_BASE, create(Type.LONG), null, null),
        new Field(LONG_HALF_DAY_AFTER, create(Type.LONG), null, null),
        new Field(LONG_ONE_DAY_AFTER, create(Type.LONG), null, null),
        new Field(LONG_ONE_WEEK_AFTER, create(Type.LONG), null, null),
        new Field(LONG_ONE_MONTH_AFTER, create(Type.LONG), null, null),
        new Field(LONG_ONE_QUARTER_AFTER, create(Type.LONG), null, null),
        new Field(LONG_ONE_YEAR_AFTER, create(Type.LONG), null, null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    ISOChronology chronology = ISOChronology.getInstanceUTC();
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      long tsBaseLong = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
      for (int i = 0; i < getCountStarResult(); i++) {
        // Generate data
        long tsHalfDayAfter = DateTimeUtils.getTimestampField(chronology, "HOUR").add(tsBaseLong, 12);
        long tsOneDayAfter = DateTimeUtils.getTimestampField(chronology, "DAY").add(tsBaseLong, 1);
        long tsOneWeekAfter = DateTimeUtils.getTimestampField(chronology, "WEEK").add(tsBaseLong, 1);
        long tsOneMonthAfter = DateTimeUtils.getTimestampField(chronology, "MONTH").add(tsBaseLong, 1);
        long tsOneQuarterAfter = DateTimeUtils.getTimestampField(chronology, "QUARTER").add(tsBaseLong, 1);
        long tsOneYearAfter = DateTimeUtils.getTimestampField(chronology, "YEAR").add(tsBaseLong, 1);

        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(TIMESTAMP_BASE, tsBaseLong);
        record.put(TIMESTAMP_HALF_DAY_AFTER, tsHalfDayAfter);
        record.put(TIMESTAMP_ONE_DAY_AFTER, tsOneDayAfter);
        record.put(TIMESTAMP_ONE_WEEK_AFTER, tsOneWeekAfter);
        record.put(TIMESTAMP_ONE_MONTH_AFTER, tsOneMonthAfter);
        record.put(TIMESTAMP_ONE_QUARTER_AFTER, tsOneQuarterAfter);
        record.put(TIMESTAMP_ONE_YEAR_AFTER, tsOneYearAfter);
        record.put(LONG_BASE, tsBaseLong);
        record.put(LONG_HALF_DAY_AFTER, tsHalfDayAfter);
        record.put(LONG_ONE_DAY_AFTER, tsOneDayAfter);
        record.put(LONG_ONE_WEEK_AFTER, tsOneWeekAfter);
        record.put(LONG_ONE_MONTH_AFTER, tsOneMonthAfter);
        record.put(LONG_ONE_QUARTER_AFTER, tsOneQuarterAfter);
        record.put(LONG_ONE_YEAR_AFTER, tsOneYearAfter);

        // add avro record to file
        fileWriter.append(record);
        tsBaseLong += 86400000;
      }
    }
    return avroFile;
  }
}
