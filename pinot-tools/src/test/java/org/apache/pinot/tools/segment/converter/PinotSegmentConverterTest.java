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
package org.apache.pinot.tools.segment.converter;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PinotSegmentConverterTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PinotSegmentConverterTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_SV_COLUMN = "intSVColumn";
  private static final String LONG_SV_COLUMN = "longSVColumn";
  private static final String FLOAT_SV_COLUMN = "floatSVColumn";
  private static final String DOUBLE_SV_COLUMN = "doubleSVColumn";
  private static final String STRING_SV_COLUMN = "stringSVColumn";
  private static final String BYTES_SV_COLUMN = "bytesSVColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_SV_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_SV_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_SV_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_SV_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_SV_COLUMN, DataType.BYTES).addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG).addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING).
          build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private String _segmentDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    GenericRow record = new GenericRow();
    record.putValue(INT_SV_COLUMN, 1);
    record.putValue(LONG_SV_COLUMN, 2L);
    record.putValue(FLOAT_SV_COLUMN, 3.0f);
    record.putValue(DOUBLE_SV_COLUMN, 4.0);
    record.putValue(STRING_SV_COLUMN, "5");
    record.putValue(BYTES_SV_COLUMN, new byte[]{6, 12, 34, 56});
    record.putValue(INT_MV_COLUMN, new Object[]{7, 8});
    record.putValue(LONG_MV_COLUMN, new Object[]{9L, 10L});
    record.putValue(FLOAT_MV_COLUMN, new Object[]{11.0f, 12.0f});
    record.putValue(DOUBLE_MV_COLUMN, new Object[]{13.0, 14.0});
    record.putValue(STRING_MV_COLUMN, new Object[]{"15", "16"});

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(Collections.singletonList(record)));
    driver.build();

    _segmentDir = driver.getOutputDirectory().getPath();
  }

  @Test
  public void testAvroConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.avro");
    PinotSegmentToAvroConverter avroConverter = new PinotSegmentToAvroConverter(_segmentDir, outputFile.getPath());
    avroConverter.convert();

    try (AvroRecordReader recordReader = new AvroRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), 1);
      assertEquals(record.getValue(LONG_SV_COLUMN), 2L);
      assertEquals(record.getValue(FLOAT_SV_COLUMN), 3.0f);
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), 4.0);
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), new byte[]{6, 12, 34, 56});
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{7, 8});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{9L, 10L});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{11.0f, 12.0f});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{13.0, 14.0});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testCsvConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.csv");
    PinotSegmentToCsvConverter csvConverter =
        new PinotSegmentToCsvConverter(_segmentDir, outputFile.getPath(), CSVRecordReaderConfig.DEFAULT_DELIMITER,
            CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER, true);
    csvConverter.convert();

    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), "1");
      assertEquals(record.getValue(LONG_SV_COLUMN), "2");
      assertEquals(record.getValue(FLOAT_SV_COLUMN), "3.0");
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), "4.0");
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), BytesUtils.toHexString(new byte[]{6, 12, 34, 56}));
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{"7", "8"});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{"9", "10"});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{"11.0", "12.0"});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{"13.0", "14.0"});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testJsonConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.json");
    PinotSegmentToJsonConverter jsonConverter = new PinotSegmentToJsonConverter(_segmentDir, outputFile.getPath());
    jsonConverter.convert();

    try (JSONRecordReader recordReader = new JSONRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), 1);
      assertEquals(record.getValue(LONG_SV_COLUMN), 2);
      assertEquals(record.getValue(FLOAT_SV_COLUMN), 3.0);
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), 4.0);
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), BytesUtils.toHexString(new byte[]{6, 12, 34, 56}));
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{7, 8});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{9, 10});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{11.0, 12.0});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{13.0, 14.0});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
