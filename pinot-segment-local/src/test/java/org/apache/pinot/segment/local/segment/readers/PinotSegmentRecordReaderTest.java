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
package org.apache.pinot.segment.local.segment.readers;

import com.sun.jna.platform.win32.WinDef;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Tests the PinotSegmentRecordReader to check that the records being generated
/// are the same as the records used to create the segment
public class PinotSegmentRecordReaderTest {
  private static final int NUM_ROWS = 10000;
  private static final String D_SV_1 = "d_sv_1";
  private static final String D_MV_1 = "d_mv_1";
  private static final String M1 = "m1";
  private static final String M2 = "m2";
  private static final String TIME = "t";
  private static final String D_SV_PK_1 = "d_sv_pk_1";
  private static final String D_SV_PK_2 = "d_sv_pk_2";
  private static final String D_SV_PK_3 = "d_sv_pk_3";

  private String _segmentOutputDir;
  private File _segmentIndexDir;
  private File _rawNoDictSegmentIndexDir;
  private List<GenericRow> _rows;
  private RecordReader _recordReader;

  @BeforeClass
  public void setup()
      throws Exception {
    Schema schema = createPinotSchema();
    TableConfig tableConfig = createTableConfig();
    String segmentName = "pinotSegmentRecordReaderTest";
    _segmentOutputDir = Files.createTempDirectory("pinot-test-").toFile().toString();
    _rows = PinotSegmentUtil.createTestData(schema, NUM_ROWS);
    _recordReader = new GenericRowRecordReader(_rows);
    _segmentIndexDir =
        PinotSegmentUtil.createSegment(tableConfig, schema, segmentName, _segmentOutputDir, _recordReader);

    TableConfig rawNoDictTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME)
            .setNoDictionaryColumns(List.of(D_SV_1)).build();
    _rawNoDictSegmentIndexDir = PinotSegmentUtil.createSegment(rawNoDictTableConfig, schema,
        segmentName + "_raw_no_dict", _segmentOutputDir, new GenericRowRecordReader(_rows));
  }

  private Schema createPinotSchema() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("schema").addSingleValueDimension(D_SV_1, DataType.STRING)
        .addMultiValueDimension(D_MV_1, DataType.STRING).addMetric(M1, DataType.INT).addMetric(M2, DataType.FLOAT)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, TIME), null)
        .addSingleValueDimension(D_SV_PK_1, DataType.STRING).addSingleValueDimension(D_SV_PK_2, DataType.BYTES)
        .addSingleValueDimension(D_SV_PK_3, DataType.INT).build();
    schema.setPrimaryKeyColumns(List.of(D_SV_PK_1, D_SV_PK_2, D_SV_PK_3));
    return schema;
  }

  private TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME).build();
  }

  @Test
  public void testPinotSegmentRecordReader()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_segmentIndexDir)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of _rows returned by PinotSegmentRecordReader is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = _rows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
      Assert.assertTrue(PinotSegmentUtil.compareMultiValueColumn(outputRow.getValue(D_MV_1), row.getValue(D_MV_1)));
      Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
      Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
      Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_1), row.getValue(D_SV_PK_1));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_2), row.getValue(D_SV_PK_2));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_3), row.getValue(D_SV_PK_3));
    }
  }

  @Test
  public void testPinotSegmentRecordReaderSortedColumn()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();
    List<String> sortOrder = new ArrayList<>();
    sortOrder.add(D_SV_1);

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_segmentIndexDir, null,
        sortOrder)) {
      while (pinotSegmentRecordReader.hasNext()) {
        GenericRow row = pinotSegmentRecordReader.next();
        outputRows.add(row);
      }
    }
    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of _rows returned by PinotSegmentRecordReader is incorrect");

    // Check that the _rows are sorted based on sorted column
    GenericRow prev = outputRows.get(0);
    for (int i = 1; i < outputRows.size(); i++) {
      GenericRow current = outputRows.get(i);
      Assert.assertTrue(((String) prev.getValue(D_SV_1)).compareTo((String) current.getValue(D_SV_1)) <= 0);
      prev = current;
    }
  }

  @Test
  public void testPinotSegmentRecordReaderSortedRawStringColumn()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();
    List<String> sortOrder = new ArrayList<>();
    sortOrder.add(D_SV_1);

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_rawNoDictSegmentIndexDir,
        null, sortOrder)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of rows returned by PinotSegmentRecordReader is incorrect");

    GenericRow prev = outputRows.get(0);
    for (int i = 1; i < outputRows.size(); i++) {
      GenericRow current = outputRows.get(i);
      Assert.assertTrue(((String) prev.getValue(D_SV_1)).compareTo((String) current.getValue(D_SV_1)) <= 0);
      prev = current;
    }
  }

  @Test
  public void testPinotSegmentRecordReaderForwardIndexOnly()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();

    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader();
    pinotSegmentRecordReader.init(_segmentIndexDir, null, null, false, true);
    try (pinotSegmentRecordReader) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of rows returned by PinotSegmentRecordReader with forwardIndexOnly is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = _rows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
      Assert.assertTrue(PinotSegmentUtil.compareMultiValueColumn(outputRow.getValue(D_MV_1), row.getValue(D_MV_1)));
      Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
      Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
      Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_1), row.getValue(D_SV_PK_1));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_2), row.getValue(D_SV_PK_2));
      Assert.assertEquals(outputRow.getValue(D_SV_PK_3), row.getValue(D_SV_PK_3));
    }
  }

  @Test
  public void testPinotSegmentRecordReaderReadPrimaryKeysOnly()
      throws Exception {
    GenericRow outputRow = null;

    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader();
    pinotSegmentRecordReader.init(_segmentIndexDir, null, null, false, true, List.of(D_SV_PK_1, D_SV_PK_2, D_SV_PK_3));

    try (pinotSegmentRecordReader) {
      int docId = 0;
      while (pinotSegmentRecordReader.hasNext()) {
        outputRow = pinotSegmentRecordReader.next();
        Object[] primaryKeys = pinotSegmentRecordReader.getPrimaryKeys(docId);
        Assert.assertEquals(outputRow.getValue(D_SV_PK_1), primaryKeys[0]);
        byte[] primaryKeyBytes = (byte[]) outputRow.getValue(D_SV_PK_2);
        ByteArray byteArray = new ByteArray(primaryKeyBytes);
        Assert.assertEquals(byteArray, primaryKeys[1]);
        Assert.assertEquals(outputRow.getValue(D_SV_PK_3), primaryKeys[2]);
        docId++;
      }
    }
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(_segmentOutputDir));
  }
}
