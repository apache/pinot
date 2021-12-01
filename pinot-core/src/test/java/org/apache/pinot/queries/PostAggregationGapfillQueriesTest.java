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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Queries test for PostAggregationGapfill queries.
 */
@SuppressWarnings("rawtypes")
public class PostAggregationGapfillQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PostAggregationGapfillQueriesTest");
  private static final String RAW_TABLE_NAME = "parkingData";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_LOTS = 4;

  private static final String IS_OCCUPIED_COLUMN = "isOccupied";
  private static final String LOT_ID_COLUMN = "lotId";
  private static final String EVENT_TIME_COLUMN = "eventTime";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(IS_OCCUPIED_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(LOT_ID_COLUMN, DataType.STRING)
      .addSingleValueDimension(EVENT_TIME_COLUMN, DataType.LONG)
      .setPrimaryKeyColumns(Arrays.asList(LOT_ID_COLUMN, EVENT_TIME_COLUMN))
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: Use a match all filter to switch between DictionaryBasedAggregationOperator and AggregationOperator
    return " WHERE eventTime >= 0";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    long current = 1636286400000L; //November 7, 2021 12:00:00 PM
    int duplicates = 16;
    int interval = 1000 * 900; // 15 minutes
    long start = current - duplicates * 2 * interval; //November 7, 2021 4:00:00 AM

    List<GenericRow> records = new ArrayList<>(NUM_LOTS * 2);
    for (int i = 0; i < NUM_LOTS; i++) {
      for (int j = 0; j < duplicates; j++) {
        if (j == 4 || j == 5 || j == 6 || j == 7 || j == 10 || j == 11) {
          continue;
        }
        long parkingTime = start + interval * 2 * j + RANDOM.nextInt(interval);
        long departingTime = j == 3 ? start + interval * (2 * j + 6) + RANDOM.nextInt(interval) : start
            + interval * (2 * j + 1) + RANDOM.nextInt(interval);

        GenericRow parkingRow = new GenericRow();
        parkingRow.putValue(EVENT_TIME_COLUMN, parkingTime);
        parkingRow.putValue(LOT_ID_COLUMN, "LotId_" + String.valueOf(i));
        parkingRow.putValue(IS_OCCUPIED_COLUMN, true);
        records.add(parkingRow);

        GenericRow departingRow = new GenericRow();
        departingRow.putValue(EVENT_TIME_COLUMN, departingTime);
        departingRow.putValue(LOT_ID_COLUMN, "LotId_" + String.valueOf(i));
        departingRow.putValue(IS_OCCUPIED_COLUMN, false);
        records.add(departingRow);
      }
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment);
  }

  @Test
  public void datetimeconvertGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 24);

    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter
        = new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 03:00:00.000");
    for (int i = 0; i < 32; i += 4) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochHours(eventTime) AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 24);

    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "'454515',  '454524', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454515");
    for (int i = 0; i < 32; i += 4) {
      Long firstTimeCol = (Long) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol.toString());
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochMinutesRounded(eventTime, 60) AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 24);

    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(ToEpochMinutesRounded(eventTime, 60), '1:HOURS:EPOCH', "
        + "'454515',  '454524', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454515");
    for (int i = 0; i < 32; i += 4) {
      Long firstTimeCol = (Long) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol.toString());
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochMinutesBucket(eventTime, 60) AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 24);

    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "'454515',  '454524', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454515");
    for (int i = 0; i < 32; i += 4) {
      Long firstTimeCol = (Long) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol.toString());
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "DATETRUNC('hour', eventTime, 'milliseconds') AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 24);

    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:HOURS:EPOCH', "
        + "'454515',  '454524', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454515");
    for (int i = 0; i < 32; i += 4) {
      Long firstTimeCol = (Long) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol.toString());
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestWithoutTimeBucketOrdering() {
    try {
      String gapfillQuery = "SELECT "
          + "PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
          + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
          + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
          + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col, "
          + "lotId, "
          + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
          + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
          + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
          + "FROM parkingData "
          + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
          + "GROUP BY 1, 2 "
          + "LIMIT 200";

      getBrokerResponseForSqlQuery(gapfillQuery);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(), "PostAggregateGapFill does not work if the time bucket is not ordered.");
    }
  }

  @Test
  public void datetimeconvertGapfillTestWithHavingClause() {
    String dataTimeConvertQueryWithUnoccupied = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "HAVING status = 'false' "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponseWithUnoccupied
        = getBrokerResponseForSqlQuery(dataTimeConvertQueryWithUnoccupied);

    ResultTable dateTimeConvertResultTableWithUnoccupied = dateTimeConvertBrokerResponseWithUnoccupied.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTableWithUnoccupied.getRows().size(), 20);

    String dataTimeConvertQueryWithOccupied = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "lotId, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "HAVING status = 'true' "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponseWithOccupied
        = getBrokerResponseForSqlQuery(dataTimeConvertQueryWithOccupied);

    ResultTable dateTimeConvertResultTableWithOccupied = dateTimeConvertBrokerResponseWithOccupied.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTableWithOccupied.getRows().size(), 4);

    String gapfillQueryWithOccupied = "SELECT "
        + "PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "HAVING status = 'true' "
        + "ORDER BY 1 "
        + "LIMIT 7";

    BrokerResponseNative gapfillBrokerResponseWithOccupied = getBrokerResponseForSqlQuery(gapfillQueryWithOccupied);

    ResultTable gapFillResultTableWithOccupied = gapfillBrokerResponseWithOccupied.getResultTable();
    Assert.assertEquals(gapFillResultTableWithOccupied.getRows().size(), 7);

    for (Object [] row : gapFillResultTableWithOccupied.getRows()) {
      Assert.assertEquals(row[2], true);
    }

    String gapfillQueryWithUnoccupied = "SELECT "
        + "PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "HAVING status = 'false' "
        + "ORDER BY 1 "
        + "LIMIT 24";

    BrokerResponseNative gapfillBrokerResponseWithUnoccupied = getBrokerResponseForSqlQuery(gapfillQueryWithUnoccupied);

    ResultTable gapFillResultTableWithUnoccupied = gapfillBrokerResponseWithUnoccupied.getResultTable();
    Assert.assertEquals(gapFillResultTableWithUnoccupied.getRows().size(), 24);
    for (Object [] row : gapFillResultTableWithUnoccupied.getRows()) {
      Assert.assertEquals(row[2], false);
    }
  }


  @Test
  public void datetimeconvertGapfillTestTimeBucketAsLastSelection() {
    String gapfillQuery = "SELECT "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3, "
        + "lotId, PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 4, 5 "
        + "ORDER BY 5 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter
        = new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 03:00:00.000");
    for (int i = 0; i < 32; i += 4) {
      String timeCol = (String) gapFillRows.get(i)[4];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(timeCol);
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[3]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[4], gapFillRows.get(i + j)[4]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[3]));
        lots.add((String) gapFillRows.get(i + j)[3]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillWithOrderingByTwoColumnsTest() {
    String gapfillQuery = "SELECT "
        + "PostAggregateGapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "'2021-11-07 3:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS') AS time_col, "
        + "lotId, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_PREVIOUS_VALUE') as status1, "
        + "FILL(lastWithTime(isOccupied, eventTime, 'BOOLEAN'), 'FILL_DEFAULT_VALUE') as status2, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN') as status3 "
        + "FROM parkingData "
        + "WHERE eventTime >= 1635940800000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1, 2 "
        + "ORDER BY 1, 2 "
        + "LIMIT 200";

    DateTimeFormatSpec dateTimeFormatter
        = new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 32);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 03:00:00.000");
    for (int i = 0; i < 32; i += 4) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Set<String> lots = new HashSet<>();
      lots.add((String) gapFillRows.get(i)[1]);
      for (int j = 1; j < 4; j++) {
        Assert.assertEquals(gapFillRows.get(i)[0], gapFillRows.get(i + j)[0]);
        Assert.assertFalse(lots.contains(gapFillRows.get(i + j)[1]));
        lots.add((String) gapFillRows.get(i + j)[1]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
