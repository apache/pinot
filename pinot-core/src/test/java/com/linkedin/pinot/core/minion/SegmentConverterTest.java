/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.minion;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentConverterTest {
  private static final File WORKING_DIR = new File(FileUtils.getTempDirectory(), "SegmentConverterTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(WORKING_DIR, "originalSegment");

  private static final int NUM_ROWS = 10000;
  private static final int REPEAT_ROWS = 5;
  private static final int NUM_SEGMENTS = 10;
  private static final String TABLE_NAME = "testTable";
  private static final String INPUT_SEGMENT_NAME_PREFIX = "testSegment_";
  private static final String D1 = "d1";
  private static final String D2 = "d2";
  private static final String M1 = "m1";
  private static final String T = "t";

  private List<File> _segmentIndexDirList;
  private final long _referenceTimestamp = System.currentTimeMillis();

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
    _segmentIndexDirList = new ArrayList<>(NUM_SEGMENTS);

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(D1, FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec(D2, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(M1, FieldSpec.DataType.INT));
    schema.addField(new TimeFieldSpec(T, FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    long timestamp = _referenceTimestamp;
    for (int i = 0; i < NUM_ROWS; i++) {
      int dimensionValue = i % (NUM_ROWS / REPEAT_ROWS);
      GenericRow row = new GenericRow();
      row.putField(D1, dimensionValue);
      row.putField(D2, Integer.toString(dimensionValue));
      row.putField(M1, dimensionValue);
      row.putField(T, timestamp++);
      rows.add(row);
    }

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = INPUT_SEGMENT_NAME_PREFIX + i;
      RecordReader recordReader = new GenericRowRecordReader(rows, schema);

      SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();

      _segmentIndexDirList.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }
  }

  @Test
  public void testSegmentConcatenate() throws Exception {
    SegmentConverter segmentConverter = new SegmentConverter.Builder()
        .setTableName(TABLE_NAME)
        .setSegmentName("segmentConcatenate")
        .setInputIndexDirs(_segmentIndexDirList)
        .setWorkingDir(WORKING_DIR)
        .setRecordTransformer((row) -> row)
        .setTotalNumPartition(1)
        .build();

    List<File> result = segmentConverter.convertSegment();

    Assert.assertEquals(result.size(), 1);
    List<GenericRow> outputRows = new ArrayList<>();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(result.get(0))) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }
    // Check that the segment is correctly rolled up on the time column
    Assert.assertEquals(outputRows.size(), NUM_ROWS * NUM_SEGMENTS,
        "Number of rows returned by segment converter is incorrect");

    // Check the value
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      int rowCount = 0;
      long timestamp = _referenceTimestamp;
      for (int j = 0; j < NUM_ROWS; j++) {
        int expectedValue = rowCount % (NUM_ROWS / REPEAT_ROWS);
        Assert.assertEquals(outputRows.get(rowCount).getValue(D1), expectedValue);
        Assert.assertEquals(outputRows.get(rowCount).getValue(D2), Integer.toString(expectedValue));
        Assert.assertEquals(outputRows.get(rowCount).getValue(M1), expectedValue);
        Assert.assertEquals(outputRows.get(rowCount).getValue(T), timestamp++);
        rowCount++;
      }
    }
  }

  @Test
  public void testSegmentRollupWithTimeConversion() throws Exception {
    final BaseDateTimeTransformer dateTimeTransformer =
        DateTimeTransformerFactory.getDateTimeTransformer("1:MILLISECONDS:EPOCH", "1:DAYS:EPOCH", "1:DAYS");

    SegmentConverter segmentConverter = new SegmentConverter.Builder()
        .setTableName(TABLE_NAME)
        .setSegmentName("segmentRollupWithTimeConversion")
        .setInputIndexDirs(_segmentIndexDirList)
        .setWorkingDir(WORKING_DIR)
        .setRecordTransformer((row) -> {
          long[] input = new long[1];
          long[] output = new long[1];
          input[0] = (Long) row.getValue(T);
          dateTimeTransformer.transform(input, output, 1);
          row.putField(T, output[0]);
          return row;
        })
        .setGroupByColumns(Arrays.asList(new String[]{D1, D2, T}))
        .setRecordAggregator((rows) -> {
          GenericRow result = rows.get(0);
          for (int i = 1; i < rows.size(); i++) {
            GenericRow current = rows.get(i);
            Object aggregatedValue =
                ((Number) result.getValue(M1)).intValue() + ((Number) current.getValue(M1)).intValue();
            result.putField(M1, aggregatedValue);
          }
          return result;
        })
        .setTotalNumPartition(1)
        .build();

    List<File> result = segmentConverter.convertSegment();

    Assert.assertEquals(result.size(), 1);
    List<GenericRow> outputRows = new ArrayList<>();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(result.get(0))) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }
    // Check that the segment is correctly rolled up on the time column
    Assert.assertEquals(outputRows.size(), NUM_ROWS / REPEAT_ROWS,
        "Number of rows returned by segment converter is incorrect");

    // Check the value
    int expectedValue = 0;
    for (GenericRow row: outputRows) {
      Assert.assertEquals(row.getValue(D1), expectedValue);
      Assert.assertEquals(row.getValue(D2), Integer.toString(expectedValue));
      Assert.assertEquals(row.getValue(M1), expectedValue * NUM_SEGMENTS * REPEAT_ROWS);
      expectedValue++;
    }
  }

  @Test
  public void testMultipleOutput() throws Exception {
    SegmentConverter segmentConverter = new SegmentConverter.Builder()
        .setTableName(TABLE_NAME)
        .setSegmentName("segmentConcatenate")
        .setInputIndexDirs(_segmentIndexDirList)
        .setWorkingDir(WORKING_DIR)
        .setRecordTransformer((row) -> row)
        .setTotalNumPartition(3)
        .build();

    List<File> result = segmentConverter.convertSegment();

    Assert.assertEquals(result.size(), 3);

    List<List<GenericRow>> outputRows = new ArrayList<>();
    for (File resultFile : result) {
      try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(resultFile)) {
        List<GenericRow> segmentOutput = new ArrayList<>();
        while (pinotSegmentRecordReader.hasNext()) {
          segmentOutput.add(pinotSegmentRecordReader.next());
        }
        outputRows.add(segmentOutput);
      }
    }

    Assert.assertEquals(outputRows.stream().mapToInt(r -> r.size()).sum(), NUM_ROWS * NUM_SEGMENTS);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
  }
}
