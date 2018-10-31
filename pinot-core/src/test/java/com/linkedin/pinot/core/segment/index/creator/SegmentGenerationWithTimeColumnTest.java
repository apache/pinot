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

package com.linkedin.pinot.core.segment.index.creator;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentGenerationWithTimeColumnTest {
  private static final String STRING_COL_NAME = "someString";
  private static final String TIME_COL_NAME = "date";
  private static final String TIME_COL_FORMAT = "yyyyMMdd";
  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "segmentGenTest";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_ROWS = 10000;

  private Random _random = new Random(System.nanoTime());

  private long minTime;
  private long maxTime;
  private long startTime = System.currentTimeMillis();

  @BeforeMethod
  public void reset() {
    minTime = Long.MAX_VALUE;
    maxTime = Long.MIN_VALUE;
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void testSimpleDateSegmentGeneration() throws Exception {
    Schema schema = createSchema(true);
    File segmentDir = buildSegment(schema, true);
    SegmentMetadataImpl metadata = SegmentDirectory.loadSegmentMetadata(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), sdfToMillis(minTime));
    Assert.assertEquals(metadata.getEndTime(), sdfToMillis(maxTime));
  }

  @Test
  public void testEpochDateSegmentGeneration() throws Exception {
    Schema schema = createSchema(false);
    File segmentDir = buildSegment(schema, false);
    SegmentMetadataImpl metadata = SegmentDirectory.loadSegmentMetadata(segmentDir);
    Assert.assertEquals(metadata.getStartTime(), minTime);
    Assert.assertEquals(metadata.getEndTime(), maxTime);
  }

  private Schema createSchema(boolean isSimpleDate) {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(STRING_COL_NAME, FieldSpec.DataType.STRING, true));
    if (isSimpleDate) {
      schema.addField(new TimeFieldSpec(TIME_COL_NAME, FieldSpec.DataType.INT, TimeUnit.DAYS));
    } else {
      schema.addField(new TimeFieldSpec(TIME_COL_NAME, FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS));
    }
    return schema;
  }

  private File buildSegment(Schema schema, boolean isSimpleDate)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(schema.getDimensionNames());

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setTimeColumnName(TIME_COL_NAME);
    if (isSimpleDate) {
      config.setSimpleDateFormat(TIME_COL_FORMAT);
    }

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;

        value = getRandomValueForColumn(fieldSpec, isSimpleDate);
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }

  private Object getRandomValueForColumn(FieldSpec fieldSpec, boolean isSimpleDate) {
    if (fieldSpec.getName().equals(TIME_COL_NAME)) {
      return getRandomValueForTimeColumn(isSimpleDate);
    }
    return RawIndexCreatorTest.getRandomValue(_random, fieldSpec.getDataType());
  }

  private Object getRandomValueForTimeColumn(boolean isSimpleDate) {
    long randomMs = ThreadLocalRandom.current().nextLong(startTime);
    long dateColVal = randomMs;
    Object result;
    if (isSimpleDate) {
      DateTime dateTime = new DateTime(randomMs);
      LocalDateTime localDateTime = dateTime.toLocalDateTime();
      int year = localDateTime.getYear();
      int month = localDateTime.getMonthOfYear();
      int day = localDateTime.getDayOfMonth();
      String dateColStr = String.format("%04d%02d%02d", year, month, day);
      dateColVal = Integer.valueOf(dateColStr);
      result = new Integer(Integer.valueOf(dateColStr));
    } else {
      result = new Long(dateColVal);
    }

    if (dateColVal < minTime) {
      minTime = dateColVal;
    }
    if (dateColVal > maxTime) {
      maxTime = dateColVal;
    }
    return result;
  }

  private long sdfToMillis(long value) {
    DateTimeFormatter sdfFormatter = DateTimeFormat.forPattern(TIME_COL_FORMAT);
    DateTime dateTime = DateTime.parse(Long.toString(value), sdfFormatter);
    return dateTime.getMillis();
  }
}
