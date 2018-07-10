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
package com.linkedin.pinot.core.data.readers;

import com.google.common.io.Files;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the MultiplePinotSegmentRecordReader to check that the records being merged correctly
 */
public class MultiplePinotSegmentRecordReaderTest {
  private static final int NUM_ROWS = 10000;
  private static final int NUM_SEGMENTS = 5;

  private static String D_SV_1 = "d_sv_1";
  private static String D_SV_2 = "d_sv_2";
  private static String D_MV_1 = "d_mv_1";
  private static String M1 = "m1";
  private static String M2 = "m2";
  private static String TIME = "t";

  private String _segmentOutputDir;
  private List<File> _segmentIndexDirList;
  private List<List<GenericRow>> _rowsList;

  @BeforeClass
  public void setup() throws Exception {
    Schema schema = createPinotSchema();
    _segmentOutputDir = Files.createTempDir().toString();
    _rowsList = new ArrayList<>(NUM_SEGMENTS);
    _segmentIndexDirList = new ArrayList<>(NUM_SEGMENTS);

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "multiplePinotSegmentRecordReaderTest_" + i;
      List<GenericRow> rows = PinotSegmentUtil.createTestData(schema, NUM_ROWS);
      _rowsList.add(rows);
      RecordReader recordReader = new GenericRowRecordReader(rows, schema);
      _segmentIndexDirList.add(PinotSegmentUtil.createSegment(schema, segmentName, _segmentOutputDir, recordReader));
    }
  }

  private Schema createPinotSchema() {
    Schema testSchema = new Schema();
    testSchema.setSchemaName("schema");
    testSchema.addField(new DimensionFieldSpec(D_SV_1, FieldSpec.DataType.STRING, true));
    testSchema.addField(new DimensionFieldSpec(D_SV_2, FieldSpec.DataType.INT, true));
    testSchema.addField(new DimensionFieldSpec(D_MV_1, FieldSpec.DataType.STRING, false));
    testSchema.addField(new MetricFieldSpec(M1, FieldSpec.DataType.INT));
    testSchema.addField(new MetricFieldSpec(M2, FieldSpec.DataType.FLOAT));
    testSchema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, TIME)));
    return testSchema;
  }

  @Test
  public void testMultiplePinotSegmentRecordReader() throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();
    try (MultiplePinotSegmentRecordReader pinotSegmentRecordReader = new MultiplePinotSegmentRecordReader(
        _segmentIndexDirList)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), NUM_SEGMENTS * NUM_ROWS,
        "Number of rows returned by PinotSegmentRecordReader is incorrect");

    int outputRowIndex = 0;
    for (List<GenericRow> rows : _rowsList) {
      for (GenericRow row : rows) {
        GenericRow outputRow = outputRows.get(outputRowIndex++);
        Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
        Assert.assertEquals(outputRow.getValue(D_SV_2), row.getValue(D_SV_2));
        Assert.assertTrue(PinotSegmentUtil.compareMultiValueColumn(outputRow.getValue(D_MV_1), row.getValue(D_MV_1)));
        Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
        Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
        Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
      }
    }
  }

  @Test
  public void testMultiplePinotSegmentRecordReaderSortedColumn() throws Exception {
    List<String> sortOrder = new ArrayList<>();
    sortOrder.add(D_SV_1);
    sortOrder.add(D_SV_2);

    List<GenericRow> outputRows = new ArrayList<>();
    try (MultiplePinotSegmentRecordReader pinotSegmentRecordReader = new MultiplePinotSegmentRecordReader(
        _segmentIndexDirList, null, sortOrder)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), NUM_SEGMENTS * NUM_ROWS,
        "Number of rows returned by PinotSegmentRecordReader is incorrect");
    GenericRow prev = outputRows.get(0);
    for (int i = 1; i < outputRows.size(); i++) {
      GenericRow current = outputRows.get(i);
      String prevValue = (String) prev.getValue(D_SV_1);
      String currentValue = (String) current.getValue(D_SV_1);
      Assert.assertTrue(prevValue.compareTo(currentValue) <= 0);

      // Check secondary order
      if (prevValue.equals(currentValue)) {
        Integer prevSecondValue = (Integer) prev.getValue(D_SV_2);
        Integer currentSecondValue = (Integer) current.getValue(D_SV_2);
        Assert.assertTrue(prevSecondValue.compareTo(currentSecondValue) <= 0);
      }

      prev = current;
    }
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(_segmentOutputDir));
  }
}
