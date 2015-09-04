/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class TestStarTreeSegmentCreator {
  private String testName;
  private File indexDir;
  private File avroFile;

  @BeforeClass
  public void beforeClass() throws Exception {
    testName = TestStarTreeSegmentCreator.class.getSimpleName();
    indexDir = new File(System.getProperty("java.io.tmpdir"), testName);
    if (indexDir.exists()) {
      FileUtils.forceDelete(indexDir);
    }
    System.out.println("indexDir=" + indexDir);

    avroFile = new File(System.getProperty("java.io.tmpdir"), testName + ".avro");
    if (avroFile.exists()) {
      FileUtils.forceDelete(avroFile);
    }
    avroFile.deleteOnExit();
//    createSampleAvroData(avroFile, 25000000, 128);
//    createSampleAvroData(avroFile, 10 * 1024 * 1024, 128);
    createSampleAvroData(avroFile, 1024, 128);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (System.getProperty("startree.test.keep.dir") == null) {
      FileUtils.forceDelete(indexDir);
    }
  }

//  @Test(enabled = false)
  @Test(enabled = true)
  public void testCreation() throws Exception {
    BasicConfigurator.configure();

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avroFile, indexDir, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");

    // Set the star tree index config
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
//    starTreeIndexSpec.setSplitExcludes(Arrays.asList("D1", "daysSinceEpoch"));
    starTreeIndexSpec.setSplitExcludes(Arrays.asList("daysSinceEpoch"));
    starTreeIndexSpec.setMaxLeafRecords(4);
    config.getSchema().setStarTreeIndexSpecs(ImmutableList.of(starTreeIndexSpec));

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
  }

  private static void createSampleAvroData(File file, int numRecords, int numTimeBuckets) throws Exception {
    Schema schema = SchemaBuilder.builder()
        .record("TestRecord")
        .fields()
        .name("D0").prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault()
        .name("D1").prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault()
        .name("D2").prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault()
        .name("daysSinceEpoch").prop("pinotType", "TIME").type().longBuilder().endLong().noDefault()
        .name("M0").prop("pinotType", "METRIC").type().longBuilder().endLong().noDefault()
        .name("M1").prop("pinotType", "METRIC").type().doubleBuilder().endDouble().noDefault()
        .endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    fileWriter.create(schema, file);

    for (int i = 0; i < numRecords; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("D0", String.valueOf(i % 2));
      record.put("D1", String.valueOf(i % 4));
      record.put("D2", String.valueOf(i % 8));
//      record.put("D0", String.valueOf(i % 16));
//      record.put("D1", String.valueOf(i % 32));
//      record.put("D2", String.valueOf(i % 128));
      record.put("daysSinceEpoch", (long) (i % numTimeBuckets));
      record.put("M0", 1L);
      record.put("M1", 1.0);
      fileWriter.append(record);
    }

    fileWriter.close();
  }
}
