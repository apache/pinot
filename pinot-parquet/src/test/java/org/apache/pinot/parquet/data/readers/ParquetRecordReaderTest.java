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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ParquetRecordReaderTest {
  protected static final String[] COLUMNS = {"INT_SV", "INT_MV"};
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ParquetRecordReaderTest");
  private static final File DATA_FILE = new File(TEMP_DIR, "data.parquet");
  protected static final org.apache.pinot.spi.data.Schema
      SCHEMA = new org.apache.pinot.spi.data.Schema.SchemaBuilder().addMetric(COLUMNS[0], FieldSpec.DataType.INT).build();
  private static final Object[][] RECORDS = {{5, new int[]{10, 15, 20}}, {25, new int[]{30, 35, 40}}, {null, null}};
  private static final Object[] DEFAULT_VALUES = {0, new int[]{-1}};

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


  protected static void checkValue(RecordReader recordReader)
      throws Exception {
    for (Object[] expectedRecord : RECORDS) {
      GenericRow actualRecord = recordReader.next();
      GenericRow transformedRecord = actualRecord;

      int numColumns = COLUMNS.length;
      for (int i = 0; i < numColumns; i++) {
        if (expectedRecord[i] != null) {
          Assert.assertEquals(transformedRecord.getValue(COLUMNS[i]), expectedRecord[i]);
        } else {
          Assert.assertEquals(transformedRecord.getValue(COLUMNS[i]), DEFAULT_VALUES[i]);
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }
  @Test
  public void testParquetRecordReader()
      throws Exception {
    try (ParquetRecordReader recordReader = new ParquetRecordReader()) {
      recordReader.init(DATA_FILE, SCHEMA, null);
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
