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
package com.linkedin.pinot.core.data.readers;

import com.google.common.io.Files;
import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DateTimeGranularitySpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests the MultipleRollupPinotSegmentRecordReader to check that the records being merged & aggregated correctly
 */
public class MultipleRollupPinotSegmentRecordReaderTest {

  private static final int NUM_ROWS = 10000;
  private static final int REPEAT_ROWS = 5;
  private static final int NUM_SEGMENTS = 3;

  private static String D_SV_1 = "d_sv_1";
  private static String D_SV_2 = "d_sv_2";
  private static String TIME = "t";
  private static String M1 = "m1";
  private static String M2 = "m2";

  private String _segmentOutputDir;
  private List<File> _segmentIndexDirList;
  private Schema _schema;

  @BeforeClass
  public void setup() throws Exception {
    _schema = createPinotSchema();
    _segmentOutputDir = Files.createTempDir().toString();
    _segmentIndexDirList = new ArrayList<>(NUM_SEGMENTS);

    List<GenericRow> _rows = new ArrayList<>();
    long timestamp = System.currentTimeMillis();
    for (int i = 0; i < NUM_ROWS; i++) {
      int dimensionValue = i % (NUM_ROWS / REPEAT_ROWS);
      Map<String, Object> fields = new HashMap<>();
      fields.put(D_SV_1, dimensionValue);
      fields.put(D_SV_2, Integer.toString(dimensionValue));
      fields.put(M1, dimensionValue);
      fields.put(M2, (float) dimensionValue);

      fields.put(TIME, timestamp++);
      GenericRow row = new GenericRow();
      row.init(fields);
      _rows.add(row);
    }

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "multipleRollupPinotSegmentRecordReaderTest_" + i;
      RecordReader recordReader = new GenericRowRecordReader(_rows, _schema);
      _segmentIndexDirList.add(PinotSegmentUtil.createSegment(_schema, segmentName, _segmentOutputDir, recordReader));
    }
  }

  @Test
  public void testMultipleRollupPinotSegmentRecordReader() throws Exception {
    // Create roll-up record reader without any config
    List<GenericRow> outputRows = new ArrayList<>();
    List<GenericRow> rewindOutputRows = new ArrayList<>();
    try (MultipleRollupPinotSegmentRecordReader pinotSegmentRecordReader = new MultipleRollupPinotSegmentRecordReader(
        _segmentIndexDirList, _schema, null)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }

      pinotSegmentRecordReader.rewind();

      while (pinotSegmentRecordReader.hasNext()) {
        rewindOutputRows.add(pinotSegmentRecordReader.next());
      }
    }

    // Check that the segment is correctly aggregated
    Assert.assertEquals(outputRows.size(), NUM_ROWS,
        "Number of rows returned by MultipleRollupPinotSegmentRecordReader is incorrect");
    Assert.assertEquals(rewindOutputRows.size(), NUM_ROWS,
        "Number of rows returned by MultipleRollupPinotSegmentRecordReader after rewind is incorrect");

    int dimensionValue = 0;
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), dimensionValue);
      Assert.assertEquals(outputRow.getValue(D_SV_2), Integer.toString(dimensionValue));
      Assert.assertEquals(outputRow.getValue(M1), dimensionValue * NUM_SEGMENTS);
      Assert.assertEquals(outputRow.getValue(M2), (float) dimensionValue * NUM_SEGMENTS);

      GenericRow rewindOutputRow = rewindOutputRows.get(i);
      Assert.assertEquals(rewindOutputRow.getValue(D_SV_1), dimensionValue);
      Assert.assertEquals(rewindOutputRow.getValue(D_SV_2), Integer.toString(dimensionValue));
      Assert.assertEquals(rewindOutputRow.getValue(M1), dimensionValue * NUM_SEGMENTS);
      Assert.assertEquals(rewindOutputRow.getValue(M2), (float) dimensionValue * NUM_SEGMENTS);

      if (i % REPEAT_ROWS == REPEAT_ROWS - 1) {
        dimensionValue++;
      }
    }
  }

  @Test
  public void testMultipleRollupPinotSegmentRecordReaderWithTimeConversionAndMultipleAggregator() throws Exception {
    // Create the roll-up record reader with aggregator type maps and and epoch time transformer
    Map<String, String> aggregatorTypeMap = new HashMap<>();
    aggregatorTypeMap.put(M1, "SUM");
    aggregatorTypeMap.put(M2, "max");

    EpochToEpochTransformer transformer = new EpochToEpochTransformer(new DateTimeFormatSpec("1:MILLISECONDS:EPOCH"),
        new DateTimeFormatSpec("1:DAYS:EPOCH"), new DateTimeGranularitySpec("1:DAYS"));

    MultipleRollupPinotSegmentRecordReaderConfig config = new MultipleRollupPinotSegmentRecordReaderConfig();
    config.setDateTimeTransformer(transformer);
    config.setAggregatorTypeMap(aggregatorTypeMap);

    List<GenericRow> outputRows = new ArrayList<>();
    try (MultipleRollupPinotSegmentRecordReader pinotSegmentRecordReader = new MultipleRollupPinotSegmentRecordReader(
        _segmentIndexDirList, _schema, config)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    // Check that the segment is correctly rolled up on the time column
    Assert.assertEquals(outputRows.size(), NUM_ROWS / REPEAT_ROWS,
        "Number of rows returned by MultipleRollupPinotSegmentRecordReader is incorrect");

    int dimensionValue = 0;
    for (GenericRow outputRow : outputRows) {
      Assert.assertEquals(outputRow.getValue(D_SV_1), dimensionValue);
      Assert.assertEquals(outputRow.getValue(D_SV_2), Integer.toString(dimensionValue));
      Assert.assertEquals(outputRow.getValue(M1), dimensionValue * NUM_SEGMENTS * REPEAT_ROWS);
      Assert.assertEquals(outputRow.getValue(M2), (float) dimensionValue);
      dimensionValue++;
    }
  }

  private Schema createPinotSchema() {
    Schema testSchema = new Schema();
    testSchema.setSchemaName("schema");
    testSchema.addField(new DimensionFieldSpec(D_SV_1, FieldSpec.DataType.INT, true));
    testSchema.addField(new DimensionFieldSpec(D_SV_2, FieldSpec.DataType.STRING, true));
    testSchema.addField(new MetricFieldSpec(M1, FieldSpec.DataType.INT));
    testSchema.addField(new MetricFieldSpec(M2, FieldSpec.DataType.FLOAT));
    testSchema.addField(
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME)));
    return testSchema;
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(_segmentOutputDir));
  }
}
