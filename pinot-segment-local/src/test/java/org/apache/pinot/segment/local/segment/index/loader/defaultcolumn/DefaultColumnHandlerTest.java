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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import java.io.File;
import java.net.URL;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class DefaultColumnHandlerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), DefaultColumnHandlerTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);
  private static final String AVRO_DATA = "data/test_data-mv.avro";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private Schema _schema;
  private SegmentDirectory _segmentDirectory;
  private SegmentDirectory.Writer _writer;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    _schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(avroFile);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, _schema);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testComputeDefaultColumnActionMap()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // Same schema
      testComputeDefaultColumnActionMap(Map.of());

      // Add single-value dimension in the schema
      _schema.addField(new DimensionFieldSpec("column11", DataType.INT, true));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DIMENSION));
      _schema.removeField("column11");

      // Add multi-value dimension in the schema
      _schema.addField(new DimensionFieldSpec("column11", DataType.INT, false));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DIMENSION));
      _schema.removeField("column11");

      // Add metric in the schema
      _schema.addField(new MetricFieldSpec("column11", DataType.INT));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_METRIC));
      _schema.removeField("column11");

      // Add date-time in the schema
      _schema.addField(new DateTimeFieldSpec("column11", DataType.INT, "EPOCH|HOURS", "1:HOURS"));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DATE_TIME));
      _schema.removeField("column11");

      // Do not remove non-autogenerated column in the segmentMetadata
      _schema.removeField("column2");
      testComputeDefaultColumnActionMap(Map.of());

      // Do not update non-autogenerated column in the schema
      _schema.addField(new DimensionFieldSpec("column2", DataType.STRING, true));
      testComputeDefaultColumnActionMap(Map.of());
    }
  }

  private void testComputeDefaultColumnActionMap(Map<String, DefaultColumnAction> expected) {
    BaseDefaultColumnHandler defaultColumnHandler =
        new V3DefaultColumnHandler(INDEX_DIR, _segmentDirectory.getSegmentMetadata(),
            new IndexLoadingConfig(TABLE_CONFIG, _schema), _schema, _writer);
    assertEquals(defaultColumnHandler.computeDefaultColumnActionMap(), expected);
  }
}
