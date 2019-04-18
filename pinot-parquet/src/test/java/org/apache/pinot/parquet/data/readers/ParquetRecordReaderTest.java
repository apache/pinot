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
package org.apache.pinot.parquet.data.readers;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.pinot.core.data.readers.RecordReaderTest;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ParquetRecordReaderTest extends RecordReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ParquetRecordReaderTest");
  private static final File DATA_FILE = new File(TEMP_DIR, "data.parquet");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);

    String strSchema =
        "{\n" + "    \"name\": \"AvroParquetTest\",\n" + "    \"type\": \"record\",\n" + "    \"fields\": [\n"
            + "        {\n" + "            \"name\": \"INT_SV\",\n" + "            \"type\": [ \"int\", \"null\"],\n"
            + "            \"default\": 0 \n" + "        },\n" + "        {\n" + "            \"name\": \"INT_MV\",\n"
            + "            \"type\": [{\n" + "                \"type\": \"array\",\n"
            + "                \"items\": \"int\"\n" + "             }, \"null\"]\n" + "        }\n" + "    ]\n" + "}";

    Schema schema = new Schema.Parser().parse(strSchema);
    List<GenericRecord> records = new ArrayList<>();

    for (Object[] r : RECORDS) {
      GenericRecord record = new GenericData.Record(schema);
      if (r[0] != null) {
        record.put("INT_SV", r[0]);
      } else {
        record.put("INT_SV", 0);
      }

      if (r[1] != null) {
        record.put("INT_MV", r[1]);
      } else {
        record.put("INT_MV", new int[]{-1});
      }

      records.add(record);
    }

    try (ParquetWriter<GenericRecord> writer = ParquetUtils
        .getParquetWriter(new Path(DATA_FILE.getAbsolutePath()), schema)) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  @Test
  public void testParquetRecordReader()
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig();
    segmentGeneratorConfig.setInputFilePath(DATA_FILE.getAbsolutePath());
    segmentGeneratorConfig.setSchema(SCHEMA);

    try (ParquetRecordReader recordReader = new ParquetRecordReader()) {
      recordReader.init(segmentGeneratorConfig);
      checkValue(recordReader);
      recordReader.rewind();
      checkValue(recordReader);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(TEMP_DIR);
  }
}
