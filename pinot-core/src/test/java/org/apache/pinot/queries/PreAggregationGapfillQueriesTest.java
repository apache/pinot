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
 * Queries test for PreAggregationGapfill queries.
 */
@SuppressWarnings("rawtypes")
public class PreAggregationGapfillQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PostAggregationGapfillQueriesTest");
  private static final String RAW_TABLE_NAME = "parkingData";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_LOTS = 4;

  private static final String IS_OCCUPIED_COLUMN = "isOccupied";
  private static final String LOT_ID_COLUMN = "lotId";
  private static final String EVENT_TIME_COLUMN = "eventTime";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(IS_OCCUPIED_COLUMN, DataType.INT)
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

  GenericRow createRow(String time, int lotId, boolean isOccupied) {
    DateTimeFormatSpec dateTimeFormatter
        = new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    GenericRow parkingRow = new GenericRow();
    parkingRow.putValue(EVENT_TIME_COLUMN, dateTimeFormatter.fromFormatToMillis(time));
    parkingRow.putValue(LOT_ID_COLUMN, "LotId_" + String.valueOf(lotId));
    parkingRow.putValue(IS_OCCUPIED_COLUMN, isOccupied);
    return parkingRow;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_LOTS * 2);
    records.add(createRow("2021-11-07 04:11:00.000", 0, true));
    records.add(createRow("2021-11-07 05:17:00.000", 1, true));
    records.add(createRow("2021-11-07 06:25:00.000", 2, true));
    records.add(createRow("2021-11-07 07:44:00.000", 3, true));
    records.add(createRow("2021-11-07 08:44:00.000", 2, false));
    records.add(createRow("2021-11-07 09:31:00.000", 3, false));
    records.add(createRow("2021-11-07 10:17:00.000", 0, false));
    records.add(createRow("2021-11-07 11:54:00.000", 1, false));

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
        + "SUM(isOccupied) "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "LIMIT 20 ";

    DateTimeFormatSpec dateTimeFormatter
        = new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 8);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 04:00:00.000");
    for (int i = 0; i < 8; i++) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochHours(eventTime) AS time_col, "
        + "SUM(isOccupied)"
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochHours(eventTime), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "LIMIT 20 ";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 8);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < 8; i++) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      start += dateTimeGranularity.granularityToMillis();
    }
  }


  @Test
  public void toEpochMinutesRoundedHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochMinutesRounded(eventTime, 60) AS time_col, "
        + "SUM(isOccupied)"
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesRounded(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "LIMIT 20 ";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 8);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < 8; i++) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void toEpochMinutesBucketHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "ToEpochMinutesBucket(eventTime, 60) AS time_col, "
        + "SUM(isOccupied)"
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count, time_col "
        + "FROM ("
        + "  SELECT  GapFill(ToEpochMinutesBucket(eventTime, 60), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "LIMIT 20 ";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 8);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < 8; i++) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void dateTruncHoursGapfillTest() {
    String dataTimeConvertQuery = "SELECT "
        + "DATETRUNC('hour', eventTime, 'milliseconds') AS time_col, "
        + "lastWithTime(isOccupied, eventTime, 'BOOLEAN')"
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponse = getBrokerResponseForSqlQuery(dataTimeConvertQuery);

    ResultTable dateTimeConvertResultTable = dateTimeConvertBrokerResponse.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTable.getRows().size(), 8);

    String gapfillQuery = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT GapFill(DATETRUNC('hour', eventTime, 'milliseconds'), '1:HOURS:EPOCH', "
        + "    '454516',  '454524', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "LIMIT 20 ";

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec("1:HOURS:EPOCH");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");

    BrokerResponseNative gapfillBrokerResponse = getBrokerResponseForSqlQuery(gapfillQuery);

    ResultTable gapFillResultTable = gapfillBrokerResponse.getResultTable();
    Assert.assertEquals(gapFillResultTable.getRows().size(), 8);
    List<Object[]> gapFillRows = gapFillResultTable.getRows();
    long start = dateTimeFormatter.fromFormatToMillis("454516");
    for (int i = 0; i < 8; i++) {
      String firstTimeCol = (String) gapFillRows.get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
      start += dateTimeGranularity.granularityToMillis();
    }
  }

  @Test
  public void datetimeconvertGapfillTestWithHavingClause() {
    String dataTimeConvertQueryWithOccupied = "SELECT "
        + "DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "'1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS') AS time_col, "
        + "SUM(isOccupied) as occupied_slots_count "
        + "FROM parkingData "
        + "WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "GROUP BY 1 "
        + "HAVING occupied_slots_count > 0 "
        + "ORDER BY 1 "
        + "LIMIT 200";

    BrokerResponseNative dateTimeConvertBrokerResponseWithOccupied
        = getBrokerResponseForSqlQuery(dataTimeConvertQueryWithOccupied);

    ResultTable dateTimeConvertResultTableWithOccupied = dateTimeConvertBrokerResponseWithOccupied.getResultTable();
    Assert.assertEquals(dateTimeConvertResultTableWithOccupied.getRows().size(), 4);

    String gapfillQueryWithOccupied = "SELECT "
        + "time_col, SUM(isOccupied) as occupied_slots_count "
        + "FROM ("
        + "  SELECT  GapFill(DATETIMECONVERT(eventTime, '1:MILLISECONDS:EPOCH', "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:HOURS'), "
        + "    '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', "
        + "    '2021-11-07 4:00:00.000',  '2021-11-07 12:00:00.000', '1:HOURS',"
        + "     FILL(isOccupied, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(eventTime, lotId)) AS time_col, isOccupied"
        + "  FROM parkingData "
        + "  WHERE eventTime >= 1636257600000 AND eventTime <= 1636286400000 "
        + "  LIMIT 200 "
        + ") "
        + "WHERE isOccupied = 1 " // for gapfilled data on pinot broker
        + "GROUP BY time_col " // for aggregated data
        + "HAVING occupied_slots_count > 0 "
        + "LIMIT 20 ";

    BrokerResponseNative gapfillBrokerResponseWithOccupied = getBrokerResponseForSqlQuery(gapfillQueryWithOccupied);

    ResultTable gapFillResultTableWithOccupied = gapfillBrokerResponseWithOccupied.getResultTable();
    Assert.assertEquals(gapFillResultTableWithOccupied.getRows().size(), 7);

    DateTimeFormatSpec dateTimeFormatter = new DateTimeFormatSpec(
        "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");
    DateTimeGranularitySpec dateTimeGranularity = new DateTimeGranularitySpec("1:HOURS");
    long start = dateTimeFormatter.fromFormatToMillis("2021-11-07 4:00:00.000");
    for (int i = 0; i < 7; i++) {
      String firstTimeCol = (String) gapFillResultTableWithOccupied.getRows().get(i)[0];
      long timeStamp = dateTimeFormatter.fromFormatToMillis(firstTimeCol);
      Assert.assertEquals(timeStamp, start);
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
