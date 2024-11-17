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
package org.apache.pinot.segment.local.segment.index.creator;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentGenerationWithTimeColumnTest {
  private static final String STRING_COL_NAME = "someString";
  private static final String TIME_COL_NAME = "date";
  private static final String TIME_COL_FORMAT_NO_ZONE = "yyyyMMdd";
  private static final String TIME_COL_FORMAT_WITH_ZONE = "yyyyMMddZ";
  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "segmentGenTest";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_ROWS = 10000;

  private long _seed = System.nanoTime();
  private Random _random = new Random(_seed);

  private long _validMinTime = TimeUtils.getValidMinTimeMillis();
  private long _validMaxTime = TimeUtils.getValidMaxTimeMillis();
  private long _minTime;
  private long _maxTime;
  private long _startTime = System.currentTimeMillis();
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    _tableConfig = createTableConfig();
    System.out.println("Seed is: " + _seed);
  }

  @BeforeMethod
  public void reset() {
    _minTime = Long.MAX_VALUE;
    _maxTime = Long.MIN_VALUE;
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    // allow tests to fix the seed by restoring it here
    _random.setSeed(_seed);
  }

  @Test
  public void testSimpleDateSegmentGenerationNoTimeZone()
      throws Exception {
    // Require no time zone in time format and time values don't have time zone.
    Schema schema = createSchema(true, TIME_COL_FORMAT_NO_ZONE);
    File segmentDir = buildSegment(_tableConfig, schema, true, false, "");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisNoTimeZone(_minTime + ""));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisNoTimeZone(_maxTime + ""));
  }

  @Test
  public void testSimpleDateSegmentGenerationWithTimeZoneUTC()
      throws Exception {
    // Require time zone in time format and time values have 'Z' suffix, i.e. UTC time zone.
    Schema schema = createSchema(true, TIME_COL_FORMAT_WITH_ZONE);
    File segmentDir = buildSegment(_tableConfig, schema, true, false, "Z");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisWithTimeZone(_minTime + "Z"));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisWithTimeZone(_maxTime + "Z"));
  }

  @Test
  public void testSimpleDateSegmentGenerationWithTimeZoneEST()
      throws Exception {
    // Require time zone in time format and time values have '-05:00' suffix, i.e. EST time zone.
    Schema schema = createSchema(true, TIME_COL_FORMAT_WITH_ZONE);
    String timeZoneSuffix = "-05:00";
    File segmentDir = buildSegment(_tableConfig, schema, true, false, timeZoneSuffix);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisWithTimeZone(_minTime + timeZoneSuffix));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisWithTimeZone(_maxTime + timeZoneSuffix));
  }

  /**
   * Tests using DateTimeFieldSpec as time column
   */
  @Test
  public void testSimpleDateSegmentGenerationNewNoTimeZone()
      throws Exception {
    // Require no time zone in time format and time values don't have time zone.
    Schema schema = createDateTimeFieldSpecSchema(true, TIME_COL_FORMAT_NO_ZONE);
    File segmentDir = buildSegment(_tableConfig, schema, true, false, "");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisNoTimeZone(_minTime + ""));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisNoTimeZone(_maxTime + ""));
  }

  @Test
  public void testSimpleDateSegmentGenerationNewWithTimeZoneUTC()
      throws Exception {
    // Require time zone in time format and time values have 'Z' suffix, i.e. UTC time zone.
    Schema schema = createDateTimeFieldSpecSchema(true, TIME_COL_FORMAT_WITH_ZONE);
    File segmentDir = buildSegment(_tableConfig, schema, true, false, "Z");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisWithTimeZone(_minTime + "Z"));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisWithTimeZone(_maxTime + "Z"));
  }

  @Test
  public void testSimpleDateSegmentGenerationNewWithTimeZoneEST()
      throws Exception {
    // Require time zone in time format and time values have '-05:00' suffix, i.e. EST time zone.
    Schema schema = createDateTimeFieldSpecSchema(true, TIME_COL_FORMAT_WITH_ZONE);
    String timeZoneSuffix = "-05:00";
    File segmentDir = buildSegment(_tableConfig, schema, true, false, timeZoneSuffix);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillisWithTimeZone(_minTime + timeZoneSuffix));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillisWithTimeZone(_maxTime + timeZoneSuffix));
  }

  @Test
  public void testEpochDateSegmentGeneration()
      throws Exception {
    Schema schema = createSchema(false, "");
    File segmentDir = buildSegment(_tableConfig, schema, false, false, "");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), _minTime);
    Assert.assertEquals(metadata.getEndTime(), _maxTime);
  }

  @Test
  public void testSimpleDateSegmentGenerationNewWithDegenerateSeed()
      throws Exception {
    _random.setSeed(255672780506968L);
    testSimpleDateSegmentGenerationNewNoTimeZone();
  }

  @Test
  public void testEpochDateSegmentGenerationWithDegenerateSeed()
      throws Exception {
    _random.setSeed(255672780506968L);
    testEpochDateSegmentGeneration();
  }

  /**
   * Tests using DateTimeFieldSpec as time column
   */
  @Test
  public void testEpochDateSegmentGenerationNew()
      throws Exception {
    Schema schema = createDateTimeFieldSpecSchema(false, "");
    File segmentDir = buildSegment(_tableConfig, schema, false, false, "");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), _minTime);
    Assert.assertEquals(metadata.getEndTime(), _maxTime);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testSegmentGenerationWithInvalidTime()
      throws Exception {
    Schema schema = createSchema(false, "");
    buildSegment(_tableConfig, schema, false, true, "");
  }

  /**
   * Tests using DateTimeFieldSpec as time column
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testSegmentGenerationWithInvalidTimeNew()
      throws Exception {
    Schema schema = createDateTimeFieldSpecSchema(false, "");
    buildSegment(_tableConfig, schema, false, true, "");
  }

  private Schema createSchema(boolean isSimpleDate, String timeColFormat) {
    Schema.SchemaBuilder builder =
        new Schema.SchemaBuilder().addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING);
    if (isSimpleDate) {
      builder.addTime(new TimeGranularitySpec(FieldSpec.DataType.STRING, TimeUnit.DAYS,
          TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":" + timeColFormat, TIME_COL_NAME), null);
    } else {
      builder.addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COL_NAME), null);
    }
    return builder.build();
  }

  private Schema createDateTimeFieldSpecSchema(boolean isSimpleDate, String timeColFormat) {
    Schema.SchemaBuilder builder =
        new Schema.SchemaBuilder().addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING);
    if (isSimpleDate) {
      builder.addDateTime(TIME_COL_NAME, FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:" + timeColFormat,
          "1:DAYS");
    } else {
      builder.addDateTime(TIME_COL_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    }
    builder.addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
    return builder.build();
  }

  private TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME_COL_NAME).build();
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema, final boolean isSimpleDate,
      final boolean isInvalidDate, String timeZoneSuffix)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;

        value = getRandomValueForColumn(fieldSpec, isSimpleDate, isInvalidDate, timeZoneSuffix);
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }

  private Object getRandomValueForColumn(FieldSpec fieldSpec, boolean isSimpleDate, boolean isInvalidDate,
      String timeZoneSuffix) {
    if (fieldSpec.getName().equals(TIME_COL_NAME)) {
      return getRandomValueForTimeColumn(isSimpleDate, isInvalidDate, timeZoneSuffix);
    }
    return RawIndexCreatorTest.getRandomValue(_random, fieldSpec.getDataType());
  }

  @Test
  public void testMinAllowedValue() {
    long millis = _validMinTime; // is in UTC from epoch (19710101)
    DateTime dateTime = new DateTime(millis, DateTimeZone.UTC);
    LocalDateTime localDateTime = dateTime.toLocalDateTime();
    int year = localDateTime.getYear();
    int month = localDateTime.getMonthOfYear();
    int day = localDateTime.getDayOfMonth();
    Assert.assertEquals(year, 1971);
    Assert.assertEquals(month, 1);
    Assert.assertEquals(day, 1);
  }

  private Object getRandomValueForTimeColumn(boolean isSimpleDate, boolean isInvalidDate, String timeZoneSuffix) {
    long randomMs = _validMinTime + (long) (_random.nextDouble() * (_startTime - _validMinTime));
    Preconditions.checkArgument(TimeUtils.timeValueInValidRange(randomMs), "Value " + randomMs + " out of range");
    long dateColVal = randomMs;
    Object result;
    if (isInvalidDate) {
      result = new DateTime(2072, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
      return result;
    } else if (isSimpleDate) {
      DateTime dateTime = new DateTime(randomMs, DateTimeZone.UTC);
      LocalDateTime localDateTime = dateTime.toLocalDateTime();
      int year = localDateTime.getYear();
      int month = localDateTime.getMonthOfYear();
      int day = localDateTime.getDayOfMonth();
      String dateColStr = String.format("%04d%02d%02d", year, month, day);
      dateColVal = Integer.parseInt(dateColStr);
      result = (int) dateColVal;
    } else {
      result = dateColVal;
    }

    if (dateColVal < _minTime) {
      _minTime = dateColVal;
    }
    if (dateColVal > _maxTime) {
      _maxTime = dateColVal;
    }
    if (isSimpleDate) {
      return result + timeZoneSuffix;
    }
    return result;
  }

  private long sdfToMillisNoTimeZone(String value) {
    // Set UTC explicitly otherwise the local time zone is used, because
    // datetime values don't have time zone info in them.
    DateTimeFormatter sdfFormatter = DateTimeFormat.forPattern(TIME_COL_FORMAT_NO_ZONE).withZoneUTC();
    DateTime dateTime = DateTime.parse(value, sdfFormatter);
    return dateTime.getMillis();
  }

  private long sdfToMillisWithTimeZone(String value) {
    // The sdfFormatter would use the time zone info in the datetime values,
    // so no need to specify the time zone explicitly.
    DateTimeFormatter sdfFormatter = DateTimeFormat.forPattern(TIME_COL_FORMAT_WITH_ZONE);
    DateTime dateTime = DateTime.parse(value, sdfFormatter);
    return dateTime.getMillis();
  }
}
