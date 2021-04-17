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
package org.apache.pinot.core.minion;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


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
  private static final long BASE_TIMESTAMP = new DateTime(2018, 9, 5, 0, 0).getMillis();

  private List<File> _segmentIndexDirList;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
    _segmentIndexDirList = new ArrayList<>(NUM_SEGMENTS);

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT)
        .addSingleValueDimension(D2, FieldSpec.DataType.STRING).addMetric(M1, FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, T), null).build();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(T).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int dimensionValue = i % (NUM_ROWS / REPEAT_ROWS);
      GenericRow row = new GenericRow();
      row.putValue(D1, dimensionValue);
      row.putValue(D2, Integer.toString(dimensionValue));
      row.putValue(M1, dimensionValue);
      row.putValue(T, BASE_TIMESTAMP + i);
      rows.add(row);
    }

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = INPUT_SEGMENT_NAME_PREFIX + i;
      RecordReader recordReader = new GenericRowRecordReader(rows);

      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, schema);
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
    SegmentConverter segmentConverter = new SegmentConverter.Builder().setTableName(TABLE_NAME)
        .setSegmentName("segmentConcatenate").setInputIndexDirs(_segmentIndexDirList).setWorkingDir(WORKING_DIR)
        .setRecordTransformer((row) -> row).setTotalNumPartition(1).setTableConfig(_tableConfig).build();

    List<File> result = segmentConverter.convertSegment();

    assertEquals(result.size(), 1);
    List<GenericRow> outputRows = new ArrayList<>();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(result.get(0))) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }
    // Check that the segment is correctly rolled up on the time column
    assertEquals(outputRows.size(), NUM_ROWS * NUM_SEGMENTS,
        "Number of rows returned by segment converter is incorrect");

    // Check the value
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      for (int j = 0; j < NUM_ROWS; j++) {
        GenericRow row = outputRows.get(i * NUM_ROWS + j);
        int expectedValue = j % (NUM_ROWS / REPEAT_ROWS);
        assertEquals(row.getValue(D1), expectedValue);
        assertEquals(row.getValue(D2), Integer.toString(expectedValue));
        assertEquals(row.getValue(M1), expectedValue);
        assertEquals(row.getValue(T), BASE_TIMESTAMP + j);
      }
    }
  }

  @Test
  public void testSegmentRollupWithTimeConversion() throws Exception {
    SegmentConverter segmentConverter = new SegmentConverter.Builder().setTableName(TABLE_NAME)
        .setSegmentName("segmentRollupWithTimeConversion").setInputIndexDirs(_segmentIndexDirList)
        .setWorkingDir(WORKING_DIR).setSkipTimeValueCheck(false).setRecordTransformer((row) -> {
          row.putValue(T, BASE_TIMESTAMP);
          return row;
        }).setGroupByColumns(Arrays.asList(D1, D2, T)).setRecordAggregator((rows) -> {
          GenericRow result = rows.get(0);
          for (int i = 1; i < rows.size(); i++) {
            GenericRow current = rows.get(i);
            Object aggregatedValue =
                ((Number) result.getValue(M1)).intValue() + ((Number) current.getValue(M1)).intValue();
            result.putValue(M1, aggregatedValue);
          }
          return result;
        }).setTotalNumPartition(1).setTableConfig(_tableConfig).build();

    List<File> result = segmentConverter.convertSegment();

    assertEquals(result.size(), 1);
    List<GenericRow> outputRows = new ArrayList<>();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(result.get(0))) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }
    // Check that the segment is correctly rolled up on the time column
    assertEquals(outputRows.size(), NUM_ROWS / REPEAT_ROWS,
        "Number of rows returned by segment converter is incorrect");

    // Check the value
    int expectedValue = 0;
    for (GenericRow row : outputRows) {
      assertEquals(row.getValue(D1), expectedValue);
      assertEquals(row.getValue(D2), Integer.toString(expectedValue));
      assertEquals(row.getValue(M1), expectedValue * NUM_SEGMENTS * REPEAT_ROWS);
      assertEquals(row.getValue(T), BASE_TIMESTAMP);
      expectedValue++;
    }
  }

  @Test
  public void testMultipleOutput() throws Exception {
    SegmentConverter segmentConverter = new SegmentConverter.Builder().setTableName(TABLE_NAME)
        .setSegmentName("segmentConcatenate").setInputIndexDirs(_segmentIndexDirList).setWorkingDir(WORKING_DIR)
        .setRecordTransformer((row) -> row).setTotalNumPartition(3).setTableConfig(_tableConfig).build();

    List<File> result = segmentConverter.convertSegment();

    assertEquals(result.size(), 3);

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

    assertEquals(outputRows.stream().mapToInt(List::size).sum(), NUM_ROWS * NUM_SEGMENTS);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
  }
}
