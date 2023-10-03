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
import java.util.List;
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
 * Queries test for Gapfill queries.
 */
// TODO: Item 1. table alias for subquery in next PR
public class GapfillQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PostAggregationGapfillQueriesTest");
  private static final String RAW_TABLE_NAME = "parkingData";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_LOTS = 4;

  private static final String IS_OCCUPIED_COLUMN = "isOccupied";
  private static final String LEVEL_ID_COLUMN = "levelId";
  private static final String LOT_ID_COLUMN = "lotId";
  private static final String EVENT_TIME_COLUMN = "eventTime";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(IS_OCCUPIED_COLUMN, DataType.INT)
          .addSingleValueDimension(LOT_ID_COLUMN, DataType.STRING)
          .addSingleValueDimension(LEVEL_ID_COLUMN, DataType.STRING)
          .addSingleValueDimension(EVENT_TIME_COLUMN, DataType.LONG)
          .setPrimaryKeyColumns(Arrays.asList(LOT_ID_COLUMN, EVENT_TIME_COLUMN)).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

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

  GenericRow createRow(String time, int levelId, int lotId, boolean isOccupied) {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    GenericRow parkingRow = new GenericRow();
    parkingRow.putValue(EVENT_TIME_COLUMN, dateTimeFormatter.fromFormatToMillis(time));
    parkingRow.putValue(LEVEL_ID_COLUMN, "Level_" + levelId);
    parkingRow.putValue(LOT_ID_COLUMN, "LotId_" + lotId);
    parkingRow.putValue(IS_OCCUPIED_COLUMN, isOccupied);
    return parkingRow;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_LOTS * 2);
    records.add(createRow("2021-11-07 04:11:00.000", 0, 0, true));
    records.add(createRow("2021-11-07 04:21:00.000", 0, 0, true));
    records.add(createRow("2021-11-07 04:31:00.000", 1, 0, true));
    records.add(createRow("2021-11-07 05:17:00.000", 0, 1, true));
    records.add(createRow("2021-11-07 05:37:00.000", 0, 1, true));
    records.add(createRow("2021-11-07 05:47:00.000", 1, 2, true));
    records.add(createRow("2021-11-07 06:25:00.000", 0, 2, true));
    records.add(createRow("2021-11-07 06:35:00.000", 0, 2, true));
    records.add(createRow("2021-11-07 06:36:00.000", 1, 1, true));
    records.add(createRow("2021-11-07 07:44:00.000", 0, 3, true));
    records.add(createRow("2021-11-07 07:46:00.000", 1, 3, true));
    records.add(createRow("2021-11-07 07:54:00.000", 1, 3, true));
    records.add(createRow("2021-11-07 08:44:00.000", 0, 2, false));
    records.add(createRow("2021-11-07 08:44:00.000", 1, 2, false));
    records.add(createRow("2021-11-07 09:31:00.000", 0, 3, false));
    records.add(createRow("2021-11-07 09:31:00.000", 1, 3, false));
    records.add(createRow("2021-11-07 10:17:00.000", 0, 0, false));
    records.add(createRow("2021-11-07 10:33:00.000", 0, 0, false));
    records.add(createRow("2021-11-07 10:33:00.000", 1, 0, false));
    records.add(createRow("2021-11-07 11:54:00.000", 0, 1, false));
    records.add(createRow("2021-11-07 11:57:00.000", 1, 1, false));
    records.add(createRow("2023-09-07 04:01:00.000", 1, 1, false));
    records.add(createRow("2023-09-07 04:02:00.000", 1, 1, true));
    records.add(createRow("2023-09-07 05:11:00.000", 1, 1, false));
    records.add(createRow("2023-09-07 07:07:00.000", 1, 1, true));
    records.add(createRow("2023-09-07 09:37:00.000", 1, 1, false));

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
  public void datetimeconvertGapfillTestSelectSelect() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     levelId, lotId, isOccupied "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestAggregateSelect() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestGapfillAggregate() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String dataTimeConvertQuery = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "SUM(isOccupied) "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponse(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestGapfillAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1, 0};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = (String) gapFillRows1.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCountsForLevel12 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel22 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCountsForLevel12.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel12.length * 2; i += 2) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i)[2]);
      }
      firstTimeCol = (String) gapFillRows2.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestGapfillAggregateWithHavingClause() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = (String) gapFillRows1.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregate() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = (String) gapFillRows1.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i += 2) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = (String) gapFillRows2.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateWithHavingClause() {
    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = (String) gapFillRows1.get(i + 1)[0];
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestSelectSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) (gapFillRows1.get(index)[0])).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[1].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochHoursGapfillTestAggregateSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + "  levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT ToEpochHours(eventTime) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) gapFillRows1.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "  GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + " levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT ToEpochHours(eventTime) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochHoursGapfillTestGapfillAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "    isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestGapfillAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1, 0};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCountsForLevel12 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel22 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCountsForLevel12.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel12.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestGapfillAggregateWithHavingClause() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "    isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestAggregateAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochHours(eventTime) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochHours(eventTime) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestAggregateAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + " FROM ("
        + "  SELECT ToEpochHours(eventTime) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochHours(eventTime) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTestAggregateAggregateWithHavingClause() {
    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochHours(eventTime) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + ") "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestSelectSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) (gapFillRows1.get(index)[0])).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[1].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestAggregateSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "GapFill(time_col, '1:MINUTES:EPOCH', "
        + "   '27270960',  '27271440', '1:HOURS',"
        + "   FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT ToEpochMinutesRounded(eventTime, 60) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) gapFillRows1.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "GapFill(time_col, '1:MINUTES:EPOCH', "
        + "   '27270960',  '27271440', '1:HOURS',"
        + "   FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT  ToEpochMinutesRounded(eventTime, 60) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestGapfillAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestGapfillAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1, 0};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCountsForLevel12 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel22 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCountsForLevel12.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel12.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestGapfillAggregateWithHavingClause() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestAggregateAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(time_col, '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesRounded(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(time_col, '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesRounded(eventTime, 60) time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestAggregateAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(time_col, '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesRounded(eventTime, 60) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(time_col, '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesRounded(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesRoundedHoursGapfillTestAggregateAggregateWithHavingClause() {
    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(time_col, '1:MINUTES:EPOCH', "
        + "    '27270960',  '27271440', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesRounded(eventTime, 60) time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MINUTES:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    long start = dateTimeFormatter.fromFormatToMillis("27270960");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestSelectSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) (gapFillRows1.get(index)[0])).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[1].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestAggregateSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + " levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) gapFillRows1.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "  GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + " levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("454516");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestGapfillAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestGapfillAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1, 0};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCountsForLevel12 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel22 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCountsForLevel12.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel12.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestGapfillAggregateWithHavingClause() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestAggregateAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestAggregateAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "      occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTestAggregateAggregateWithHavingClause() {
    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT ToEpochMinutesBucket(eventTime, 60) AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestSelectSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) (gapFillRows1.get(index)[0])).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[1].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void dateTruncHoursGapfillTestAggregateSelect() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + " levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = ((Long) gapFillRows1.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "  GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + "  levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = ((Long) gapFillRows2.get(index)[0]).toString();
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void dateTruncHoursGapfillTestGapfillAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestGapfillAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1, 0};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCountsForLevel12 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel22 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCountsForLevel12.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel12.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows2.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel12[i / 2], gapFillRows2.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows2.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel22[i / 2], gapFillRows2.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestGapfillAggregateWithHavingClause() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT time_col, levelId, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCountsForLevel11 = new double[]{4, 5, 6, 5, 3, 2, 1};
    double[] expectedOccupiedSlotsCountsForLevel21 = new double[]{2, 3, 4, 7, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCountsForLevel11.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCountsForLevel11.length * 2; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i)[2]);
      }
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      if ("Level_0".equals(gapFillRows1.get(i + 1)[1])) {
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel11[i / 2], gapFillRows1.get(i + 1)[2]);
      } else {
        Assert.assertEquals("Level_1", gapFillRows1.get(i + 1)[1]);
        Assert.assertEquals(expectedOccupiedSlotsCountsForLevel21[i / 2], gapFillRows1.get(i + 1)[2]);
      }
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestAggregateAggregate() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), "
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col"
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[1]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[0]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestAggregateAggregateWithOptionalGroupBy() {
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), "
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), "
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col, levelId "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length * 2);
    start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows2.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows2.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTestAggregateAggregateWithHavingClause() {
    String gapfillQuery1 = "SELECT "
        + "time_col, levelId, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', "
        + "    '1636257600000',  '1636286400000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)),"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col, levelId "
        + " HAVING occupied_slots_count > 0"
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{1, 2, 3, 4, 3, 2, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length * 2);
    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    long start = dateTimeFormatter.fromFormatToMillis("1636257600000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i += 2) {
      String firstTimeCol = ((Long) gapFillRows1.get(i)[0]).toString();
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      firstTimeCol = ((Long) gapFillRows1.get(i + 1)[0]).toString();
      timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i / 2], gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestWithMissingTimeSeries() {
    String gapfillQuery = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE')) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    try {
      getBrokerResponse(gapfillQuery);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getClass().getSimpleName().equals("IllegalArgumentException"));
    }
  }

  @Test
  public void datetimeconvertGapfillTestWithMissingGroupByTimeBucket() {
    String gapfillQuery = "SELECT "
        + "levelId, SUM(isOccupied) "
        + "FROM ("
        + "  SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY levelId"
        + " LIMIT 200 ";

    try {
      getBrokerResponse(gapfillQuery);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getClass().getSimpleName().equals("IllegalArgumentException"));
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateWithLimitTesting() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 40 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 40 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 56 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 6 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestSelectSelectOrderBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     levelId, lotId, isOccupied "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "ORDER BY time_col, levelId "
        + "LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 6}, {8, 4}, {10, 2}, {12, 0}, {6, 4}, {4, 6}, {2, 10}, {0, 10}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  ORDER BY time_col, levelId DESC"
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestAggregateSelectOrderBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  ORDER BY time_col, levelId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{2, 6}, {4, 4}, {6, 2}, {8, 0}, {6, 2}, {4, 4}, {2, 6}, {0, 8}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 =
        "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
            + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
            + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
            + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
            + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
            + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
            + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
            + "  GROUP BY time_col, levelId, lotId "
        + "  ORDER BY time_col, levelId "
        + "  LIMIT 200 "
        + ") "
            + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestGapfillAggregateOrderBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String dataTimeConvertQuery = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "SUM(isOccupied) "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponse(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  ORDER BY time_col, levelId "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 8, 10, 12, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  ORDER BY time_col, levelId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 8, 10, 12, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateOrderBy() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col, levelId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{2, 4, 6, 8, 6, 4, 2, 0};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col, levelId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{2, 4, 6, 8, 6, 4, 2};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateWithTimeBucketAggregation() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, COUNT(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '5:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '5:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 2000 "
        + "  ) "
        + "  LIMIT 2000 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 2000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    long [] expectedOccupiedSlotsCounts1 = new long[]{16, 36, 60, 79, 88, 60, 33, 21};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '5:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '5:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 2000 "
        + "  ) "
        + "  LIMIT 2000 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 2000 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{16, 36, 60, 79, 88, 60, 33, 21};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestSelectSelectWithExtraData() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     levelId, lotId, isOccupied "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 =
        new int[][]{{6, 4}, {4, 6}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "time_col, levelId, lotId, isOccupied, time_col "
        + "FROM ("
        + "  SELECT GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 4};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestAggregateSelectWithExtraData() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    int[][] expectedOccupiedSlotsCounts1 = new int[][]{{6, 2}, {4, 4}};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    int index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      int ones = expectedOccupiedSlotsCounts1[i][0];
      int zeros = expectedOccupiedSlotsCounts1[i][1];
      int total = ones + zeros;
      for (int k = 0; k < total; k++) {
        String firstTimeCol = (String) gapFillRows1.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        if (gapFillRows1.get(index)[3].equals(1)) {
          ones--;
        } else {
          zeros--;
        }
        index++;
      }
      Assert.assertEquals(ones, 0);
      Assert.assertEquals(zeros, 0);
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows1.size(), index);

    String gapfillQuery2 = "SELECT "
        + "GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "    FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)), levelId, lotId, occupied "
        + "FROM ("
        + "  SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "     lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  GROUP BY time_col, levelId, lotId "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    int[] expectedOccupiedSlotsCounts2 = new int[]{6, 4};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    index = 0;
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      for (int k = 0; k < expectedOccupiedSlotsCounts2[i]; k++) {
        String firstTimeCol = (String) gapFillRows2.get(index)[0];
        long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
        Assert.assertEquals(timeStamp, start);
        Assert.assertEquals(gapFillRows2.get(index)[3], 1);
        index++;
      }
      start += dateTimeGranularity.granularityToMillis();
    }
    Assert.assertEquals(gapFillRows2.size(), index);
  }

  @Test
  public void datetimeconvertGapfillTestGapfillAggregateWithExtraData() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String dataTimeConvertQuery = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "SUM(isOccupied) "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponse(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 4};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     isOccupied, lotId, levelId"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + " WHERE isOccupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 4};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateWithExtraData() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{6, 4};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    String gapfillQuery2 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 8:00:00.000',  '2021-11-07 10:00:00.000', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " WHERE occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse2 = getBrokerResponse(gapfillQuery2);

    double[] expectedOccupiedSlotsCounts2 = new double[]{6, 4};
    ResultTable gapFillResultTable2 = gapfillBrokerResponse2.getResultTable();
    List<Object[]> gapFillRows2 = gapFillResultTable2.getRows();
    Assert.assertEquals(gapFillRows2.size(), expectedOccupiedSlotsCounts2.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 08:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts2.length; i++) {
      String firstTimeCol = (String) gapFillRows2.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts2[i], gapFillRows2.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateOutOfBoundary() {
    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', 1694066400000, 1694077200000, '1:HOURS', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1694044800000 AND eventTime <= 1694131200000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col "
        + " LIMIT 200 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double[] expectedOccupiedSlotsCounts1 = new double[]{0, 1, 1};
    ResultTable gapFillResultTable1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTable1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start = 1694066400000L;
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length; i++) {
      long timeStamp = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
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
