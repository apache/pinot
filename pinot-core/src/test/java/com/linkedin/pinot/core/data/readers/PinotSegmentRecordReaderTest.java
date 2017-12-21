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
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the PinotSegmentRecordReader to check that the records being generated
 * are the same as the records used to create the segment
 */
public class PinotSegmentRecordReaderTest {
  private static final int NUM_ROWS = 10000;

  private String segmentName;
  private Schema schema;
  private String segmentOutputDir;
  private File segmentIndexDir;
  private List<GenericRow> rows;
  private RecordReader recordReader;

  private static String D_SV_1 = "d_sv_1";
  private static String D_MV_1 = "d_mv_1";
  private static String M1 = "m1";
  private static String M2 = "m2";
  private static String TIME = "t";

  @BeforeClass
  public void setup() throws Exception {
    segmentName = "pinotSegmentRecordReaderTest";
    schema = createPinotSchema();
    segmentOutputDir = Files.createTempDir().toString();
    segmentIndexDir = new File(segmentOutputDir, segmentName);
    createTestData();
    recordReader = new GenericRowRecordReader(rows, schema);
    createSegment();
  }

  private void createTestData() {
    Random random = new Random();
    rows = new ArrayList<>(NUM_ROWS);

    Map<String, Object> fields;
    for (int i = 0; i < NUM_ROWS; i++) {
      fields = new HashMap<>();
      fields.put(D_SV_1, D_SV_1 + "_" + RandomStringUtils.randomAlphabetic(2));
      Object[] d2Array = new Object[5];
      for (int j = 0; j < 5; j++) {
        d2Array[j] = D_MV_1 + "_" + j + "_" + RandomStringUtils.randomAlphabetic(2);
      }
      fields.put(D_MV_1, d2Array);
      fields.put(M1, Math.abs(random.nextInt()));
      fields.put(M2, Math.abs(random.nextFloat()));
      fields.put(TIME, Math.abs(random.nextLong()));

      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);
    }
  }

  private Schema createPinotSchema() {
    Schema testSchema = new Schema();
    testSchema.setSchemaName("schema");
    FieldSpec spec;
    spec = new DimensionFieldSpec(D_SV_1, DataType.STRING, true);
    testSchema.addField(spec);
    spec = new DimensionFieldSpec(D_MV_1, DataType.STRING, false);
    testSchema.addField(spec);
    spec = new MetricFieldSpec(M1, DataType.INT);
    testSchema.addField(spec);
    spec = new MetricFieldSpec(M2, DataType.FLOAT);
    testSchema.addField(spec);
    spec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, TIME));
    testSchema.addField(spec);
    return testSchema;
  }

  private void createSegment() throws Exception {

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName(segmentName);
    segmentGeneratorConfig.setOutDir(segmentOutputDir);
    segmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  @Test
  public void testPinotSegmentRecordReader() throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(segmentIndexDir)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), rows.size(),
        "Number of rows returned by PinotSegmentRecordReader is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = rows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
      Assert.assertEquals(outputRow.getValue(D_MV_1), row.getValue(D_MV_1));
      Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
      Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
      Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
    }
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(segmentOutputDir));
  }
}
