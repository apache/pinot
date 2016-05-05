package com.linkedin.pinot.core.data.readers;

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

import com.google.common.base.Joiner;
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

/**
 * Tests the PinotSegmentRecordReader to check that the records being generated
 * are the same as the records used to create the segment
 */
public class PinotSegmentRecordReaderTest {

  private String segmentName;
  private Schema schema;
  private String segmentOutputDir;
  private File segmentIndexDir;
  private List<GenericRow> rows;
  private TestRecordReader recordReader;


  @BeforeClass
  public void setup() throws Exception {
    segmentName = "pinotSegmentRecordReaderTest";
    schema = createSchema();
    segmentOutputDir = Files.createTempDir().toString();
    segmentIndexDir = new File(segmentOutputDir, segmentName);
    rows = createTestData();
    recordReader = new TestRecordReader(rows, schema);
    createSegment();
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();
    Random random = new Random();

    Map<String, Object> fields;
    for (int i = 0; i < 10000; i++) {
      fields = new HashMap<>();
      fields.put("d1", "d1_" + RandomStringUtils.randomAlphabetic(2));
      Object[] d2Array = new Object[5];
      for (int j = 0; j < 5; j++) {
        d2Array[j] = "d2_" + j + "_" + RandomStringUtils.randomAlphabetic(2);
      }
      fields.put("d2", d2Array);
      fields.put("m1", Math.abs(random.nextInt()));
      fields.put("m2", Math.abs(random.nextFloat()));
      fields.put("t", Math.abs(random.nextLong()));

      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);
    }
    return rows;
  }

  private Schema createSchema() {
    Schema testSchema = new Schema();
    testSchema.setSchemaName("schema");
    FieldSpec spec;
    spec = new DimensionFieldSpec("d1", DataType.STRING, true);
    testSchema.addField("d1", spec);
    spec = new DimensionFieldSpec("d2", DataType.STRING, false, ",");
    testSchema.addField("d2", spec);
    spec = new MetricFieldSpec("m1", DataType.INT);
    testSchema.addField("m1", spec);
    spec = new MetricFieldSpec("m2", DataType.FLOAT);
    testSchema.addField("m2", spec);
    spec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "t"));
    testSchema.addField("t", spec);
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

    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(segmentIndexDir);
    pinotSegmentRecordReader.init();
    while (pinotSegmentRecordReader.hasNext()) {
      outputRows.add(pinotSegmentRecordReader.next());
    }
    pinotSegmentRecordReader.close();

    Assert.assertEquals(outputRows.size(), rows.size(), "Number of rows returned by PinotSegmentRecordReader is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = rows.get(i);
      Assert.assertEquals(outputRow.getValue("d1"), row.getValue("d1"));
      Assert.assertEquals(outputRow.getValue("d2"), row.getValue("d2"));
      Assert.assertEquals(outputRow.getValue("m1"), row.getValue("m1"));
      Assert.assertEquals(outputRow.getValue("m2"), row.getValue("m2"));
      Assert.assertEquals(outputRow.getValue("t"), row.getValue("t"));
    }
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(segmentOutputDir));
  }
}
