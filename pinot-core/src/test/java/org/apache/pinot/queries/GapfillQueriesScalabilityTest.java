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
import org.apache.pinot.common.response.broker.ResultTableRows;
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
 * Scalability Queries test for Gapfill queries.
 */
public class GapfillQueriesScalabilityTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PostAggregationGapfillQueriesTest");
  private static final String RAW_TABLE_NAME = "parkingData";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_LOTS = 400;

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

  GenericRow createRow(long time, int levelId, int lotId, boolean isOccupied) {
    GenericRow parkingRow = new GenericRow();
    parkingRow.putValue(EVENT_TIME_COLUMN, time);
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
    long start = 1636243200000L + 5000;
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < NUM_LOTS; j++) {
        for (int k = 0; k < 4; k++) {
            records.add(createRow(start + i * 3600_000, k, i * NUM_LOTS + j, true));
        }
      }
    }
    start += 9 * 3600_000;
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < NUM_LOTS; j++) {
        for (int k = 0; k < 4; k++) {
          records.add(createRow(start + i * 3600_000, k, i * NUM_LOTS + j, false));
        }
      }
    }
    start += 15 * 3600_000;
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < NUM_LOTS; j++) {
        for (int k = 0; k < 4; k++) {
          records.add(createRow(start + i * 3600_000, k, i * NUM_LOTS + j, true));
        }
      }
    }
    start += 9 * 3600_000;
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < NUM_LOTS; j++) {
        for (int k = 0; k < 4; k++) {
          records.add(createRow(start + i * 3600_000, k, i * NUM_LOTS + j, false));
        }
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
  public void datetimeconvertGapfillTestAggregateAggregateScalabilityTestCountWithLongTimeColumn() {
    DateTimeFormatSpec dateTimeFormatter =
          new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("15:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, count(occupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:EPOCH', "
        + "    '1636243200000',  '1636416000000', '15:MINUTES',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:EPOCH', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);


    long [] expectedOccupiedSlotsCounts1 = new long []{
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600,
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636243200000");

    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      long firstTimeCol = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(firstTimeCol, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("1636329600000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      long firstTimeCol = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(firstTimeCol, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateScalabilityTestCountWithStringTimeColumn() {
    DateTimeFormatSpec dateTimeFormatter =
          new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("15:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, count(occupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 00:00:00.000',  '2021-11-09 00:00:00.000', '15:MINUTES',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    long [] expectedOccupiedSlotsCounts1 = new long []{
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600,
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 00:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("2021-11-08 00:00:00.000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateScalabilityTestSumAvgWithLongTimeColumn() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("15:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, AVG(occupied) "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:EPOCH', "
        + "    '1636243200000',  '1636416000000', '15:MINUTES',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:EPOCH', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double [] expectedOccupiedSlotsCounts1 = new double []{
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600,
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636243200000");

    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      long timeStamp = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("1636329600000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      long timeStamp = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggregateAggregateScalabilityTestSumAvgWithStringTimeColumn() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("15:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, AVG(occupied) "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 00:00:00.000',  '2021-11-09 00:00:00.000', '15:MINUTES',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double [] expectedOccupiedSlotsCounts1 = new double []{
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600,
        1600, 1600, 1600, 1600,
        3200, 3200, 3200, 3200,
        4800, 4800, 4800, 4800,
        6400, 6400, 6400, 6400,
        8000, 8000, 8000, 8000,
        9600, 9600, 9600, 9600,
        11200, 11200, 11200, 11200,
        12800, 12800, 12800, 12800,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        14400, 14400, 14400, 14400,
        12800, 12800, 12800, 12800,
        11200, 11200, 11200, 11200,
        9600, 9600, 9600, 9600,
        8000, 8000, 8000, 8000,
        6400, 6400, 6400, 6400,
        4800, 4800, 4800, 4800,
        3200, 3200, 3200, 3200,
        1600, 1600, 1600, 1600};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 00:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("2021-11-08 00:00:00.000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggAggScalabilityTestCountWithLongTimeColumnWithTimeBucketAggregation() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("60:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, count(occupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:EPOCH', "
        + "    '1636243200000',  '1636416000000', '15:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:EPOCH', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);


    long [] expectedOccupiedSlotsCounts1 = new long []{
        6400, 12800, 19200, 25600, 32000, 38400, 44800, 51200,
        57600, 57600, 57600, 57600, 57600, 57600, 57600, 51200,
        44800, 38400, 32000, 25600, 19200, 12800, 6400, 6400,
        12800, 19200, 25600, 32000, 38400, 44800, 51200, 57600,
        57600, 57600, 57600, 57600, 57600, 57600, 51200, 44800,
        38400, 32000, 25600, 19200, 12800, 6400};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636243200000");

    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      long firstTimeCol = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(firstTimeCol, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("1636329600000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      long firstTimeCol = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(firstTimeCol, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggAggScalabilityTestCountWithStringTimeColumnWithTimeBucketAggregation() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("60:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, count(occupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 00:00:00.000',  '2021-11-09 00:00:00.000', '15:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    long [] expectedOccupiedSlotsCounts1 = new long []{
        6400, 12800, 19200, 25600, 32000, 38400, 44800, 51200,
        57600, 57600, 57600, 57600, 57600, 57600, 57600, 51200,
        44800, 38400, 32000, 25600, 19200, 12800, 6400, 6400,
        12800, 19200, 25600, 32000, 38400, 44800, 51200, 57600,
        57600, 57600, 57600, 57600, 57600, 57600, 51200, 44800,
        38400, 32000, 25600, 19200, 12800, 6400};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 00:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("2021-11-08 00:00:00.000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggAggScalabilityTestSumAvgWithLongTimeColumnWithTimeBucketAggregation() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("60:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, AVG(occupied) "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:EPOCH', "
        + "    '1636243200000',  '1636416000000', '15:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:EPOCH', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double [] expectedOccupiedSlotsCounts1 = new double []{
        6400, 12800, 19200, 25600, 32000, 38400, 44800, 51200,
        57600, 57600, 57600, 57600, 57600, 57600, 57600, 51200,
        44800, 38400, 32000, 25600, 19200, 12800, 6400, 6400,
        12800, 19200, 25600, 32000, 38400, 44800, 51200, 57600,
        57600, 57600, 57600, 57600, 57600, 57600, 51200, 44800,
        38400, 32000, 25600, 19200, 12800, 6400};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("1636243200000");

    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      long timeStamp = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("1636329600000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      long timeStamp = (Long) gapFillRows1.get(i)[0];
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestAggAggScalabilityTestSumAvgWithStringTimeColumnWithTimeBucketAggregation() {
    DateTimeFormatSpec dateTimeFormatter =
        new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("60:MINUTES");
    long start;

    String gapfillQuery1 = "SELECT "
        + "time_col, SUM(occupied) as occupied_slots_count, AVG(occupied) "
        + "FROM ("
        + "  SELECT GapFill(time_col, "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 00:00:00.000',  '2021-11-09 00:00:00.000', '15:MINUTES', '1:HOURS',"
        + "     FILL(occupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(levelId, lotId)) AS time_col,"
        + "     occupied, lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "      '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '15:MINUTES') AS time_col,"
        + "       lastWithTime(isOccupied, eventTime, 'INT') as occupied, lotId, levelId"
        + "    FROM parkingData "
        + "    WHERE eventTime >= 1636243200000 AND eventTime < 1636416000000 "
        + "    GROUP BY time_col, levelId, lotId "
        + "    ORDER BY time_col "
        + "    LIMIT 200000000 "
        + "  ) "
        + "  LIMIT 2000000000 "
        + ") "
        + "  where occupied = 1 "
        + " GROUP BY time_col "
        + " LIMIT 200000000 ";

    BrokerResponseNative gapfillBrokerResponse1 = getBrokerResponse(gapfillQuery1);

    double [] expectedOccupiedSlotsCounts1 = new double []{
        6400, 12800, 19200, 25600, 32000, 38400, 44800, 51200,
        57600, 57600, 57600, 57600, 57600, 57600, 57600, 51200,
        44800, 38400, 32000, 25600, 19200, 12800, 6400, 6400,
        12800, 19200, 25600, 32000, 38400, 44800, 51200, 57600,
        57600, 57600, 57600, 57600, 57600, 57600, 51200, 44800,
        38400, 32000, 25600, 19200, 12800, 6400};
    ResultTableRows gapFillResultTableRows1 = gapfillBrokerResponse1.getResultTable();
    List<Object[]> gapFillRows1 = gapFillResultTableRows1.getRows();
    Assert.assertEquals(gapFillRows1.size(), expectedOccupiedSlotsCounts1.length);
    start = dateTimeFormatter.fromFormatToMillis("2021-11-07 00:00:00.000");
    for (int i = 0; i < expectedOccupiedSlotsCounts1.length / 2; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
      start += dateTimeGranularity.granularityToMillis();
    }

    start = dateTimeFormatter.fromFormatToMillis("2021-11-08 00:00:00.000");
    for (int i = expectedOccupiedSlotsCounts1.length / 2; i < expectedOccupiedSlotsCounts1.length; i++) {
      String firstTimeCol = (String) gapFillRows1.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      Assert.assertEquals(expectedOccupiedSlotsCounts1[i], gapFillRows1.get(i)[1]);
      Assert.assertEquals(1.0, gapFillRows1.get(i)[2]);
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
