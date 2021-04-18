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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import org.apache.pinot.core.minion.rollup.MergeType;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MergeRollupSegmentConverterTest {

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
  private static final String M2 = "m2";
  private static final String T = "t";

  private List<File> _segmentIndexDirList;
  private final long _referenceTimestamp = System.currentTimeMillis();
  private TableConfig _tableConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
    _segmentIndexDirList = new ArrayList<>(NUM_SEGMENTS);

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT)
        .addSingleValueDimension(D2, FieldSpec.DataType.STRING).addMetric(M1, FieldSpec.DataType.LONG)
        .addMetric(M2, FieldSpec.DataType.DOUBLE)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, T), null).build();

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(T).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    long timestamp = _referenceTimestamp;
    for (int i = 0; i < NUM_ROWS; i++) {
      int dimensionValue = i % (NUM_ROWS / REPEAT_ROWS);
      GenericRow row = new GenericRow();
      row.putValue(D1, dimensionValue);
      row.putValue(D2, Integer.toString(dimensionValue));
      row.putValue(M1, (long) dimensionValue);
      row.putValue(M2, (double) dimensionValue);
      row.putValue(T, timestamp++);
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
  public void testSegmentConcatenate()
      throws Exception {
    // Run roll-up segment converter with "CONCATENATE" merge type
    MergeRollupSegmentConverter rollupSegmentConverter =
        new MergeRollupSegmentConverter.Builder().setInputIndexDirs(_segmentIndexDirList).setWorkingDir(WORKING_DIR)
            .setTableName(TABLE_NAME).setSegmentName("TestConcatenate").setMergeType(MergeType.CONCATENATE)
            .setTableConfig(_tableConfig).build();
    List<File> result = rollupSegmentConverter.convert();
    Assert.assertEquals(result.size(), 1);

    // Read the output result of the segment converter
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
      for (int j = 0; j < NUM_ROWS; j++) {
        int expectedValue = rowCount % (NUM_ROWS / REPEAT_ROWS);
        Assert.assertEquals(outputRows.get(rowCount).getValue(D1), expectedValue);
        Assert.assertEquals(outputRows.get(rowCount).getValue(D2), Integer.toString(expectedValue));
        Assert.assertEquals(outputRows.get(rowCount).getValue(M1), (long) expectedValue);
        Assert.assertEquals(outputRows.get(rowCount).getValue(M2), (double) expectedValue);
        rowCount++;
      }
    }
  }

  @Test
  public void testSegmentSimpleRollup()
      throws Exception {
    // Generate aggregate type map
    Map<String, String> preAggregateType = new HashMap<>();
    preAggregateType.put(M1, "SUM");

    // Run roll-up segment converter with "ROLLUP" merge type
    MergeRollupSegmentConverter rollupSegmentConverter =
        new MergeRollupSegmentConverter.Builder().setInputIndexDirs(_segmentIndexDirList).setWorkingDir(WORKING_DIR)
            .setTableName(TABLE_NAME).setSegmentName("TestSimpleRollup").setMergeType(MergeType.ROLLUP)
            .setRollupPreAggregateType(preAggregateType).setTableConfig(_tableConfig).build();
    List<File> result = rollupSegmentConverter.convert();
    Assert.assertEquals(result.size(), 1);

    // Read the output result of the segment converter
    List<GenericRow> outputRows = new ArrayList<>();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(result.get(0))) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    // Check that the segment is correctly rolled up on the time column
    Assert.assertEquals(outputRows.size(), NUM_ROWS, "Number of rows returned by segment converter is incorrect");

    // Check the value
    for (int i = 0; i < outputRows.size(); i++) {
      int expectedValue = i / REPEAT_ROWS;
      GenericRow row = outputRows.get(i);
      Assert.assertEquals(row.getValue(D1), expectedValue);
      Assert.assertEquals(row.getValue(D2), Long.toString(expectedValue));
      Assert.assertEquals(row.getValue(M1), (long) expectedValue * NUM_SEGMENTS);
      Assert.assertEquals(row.getValue(M2), (double) expectedValue * NUM_SEGMENTS);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(WORKING_DIR);
  }
}
