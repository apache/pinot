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
package org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Regression tests for [ColumnMinMaxValueGenerator] on raw BYTES columns.
///
/// The generator reads the forward index when column metadata does not contain min/max values. These tests exercise
/// both single-value and multi-value forward index paths and verify their unsigned byte-wise ordering.
public class ColumnMinMaxValueGeneratorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ColumnMinMaxValueGeneratorTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "testSegment";
  private static final String BYTES_COLUMN = "bytesCol";
  private static final String BYTES_MV_COLUMN = "bytesMvCol";

  private static final byte[] BYTES_SMALL = new byte[]{0x00, 0x01};
  private static final byte[] BYTES_MID = new byte[]{0x10, (byte) 0xff};
  private static final byte[] BYTES_LARGE = new byte[]{(byte) 0x80, 0x00};

  @Test
  public void testRawBytesMinMaxDirection()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
        .addMultiValueDimension(BYTES_MV_COLUMN, DataType.BYTES)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(List.of(BYTES_COLUMN, BYTES_MV_COLUMN))
        .build();

    File indexDir = buildSegment(tableConfig, schema, createRows());
    removeMinMaxValuesFromMetadataFile(indexDir);

    generateMinMaxValues(indexDir);

    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(indexDir);
    assertMinMax(reloaded, BYTES_COLUMN);
    assertMinMax(reloaded, BYTES_MV_COLUMN);
  }

  @Test
  public void testEmptyRawBytesMarksMinMaxInvalid()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(List.of(BYTES_COLUMN))
        .build();

    File indexDir = buildSegment(tableConfig, schema, List.of());
    removeMinMaxValuesFromMetadataFile(indexDir);
    generateMinMaxValues(indexDir);

    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(indexDir);
    assertNull(reloaded.getColumnMetadataFor(BYTES_COLUMN).getMinValue());
    assertNull(reloaded.getColumnMetadataFor(BYTES_COLUMN).getMaxValue());
    assertTrue(reloaded.getColumnMetadataFor(BYTES_COLUMN).isMinMaxValueInvalid());
  }

  private static void assertMinMax(SegmentMetadataImpl segmentMetadata, String column) {
    ByteArray min = (ByteArray) segmentMetadata.getColumnMetadataFor(column).getMinValue();
    ByteArray max = (ByteArray) segmentMetadata.getColumnMetadataFor(column).getMaxValue();
    assertEquals(min, new ByteArray(BYTES_SMALL));
    assertEquals(max, new ByteArray(BYTES_LARGE));
    assertTrue(min.compareTo(max) < 0);
  }

  private static void generateMinMaxValues(File indexDir)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ColumnMinMaxValueGenerator generator =
          new ColumnMinMaxValueGenerator(segmentMetadata, writer, ColumnMinMaxValueGeneratorMode.ALL);
      generator.addColumnMinMaxValue();
    }
  }

  private static List<GenericRow> createRows() {
    List<GenericRow> rows = new ArrayList<>();
    for (byte[] value : new byte[][]{BYTES_MID, BYTES_LARGE, BYTES_SMALL}) {
      GenericRow row = new GenericRow();
      row.putValue(BYTES_COLUMN, value);
      row.putValue(BYTES_MV_COLUMN, new Object[]{value, BYTES_MID});
      rows.add(row);
    }
    return rows;
  }

  private static File buildSegment(TableConfig tableConfig, Schema schema, List<GenericRow> rows)
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TEMP_DIR, SEGMENT_NAME);
  }

  private static void removeMinMaxValuesFromMetadataFile(File indexDir)
      throws Exception {
    PropertiesConfiguration configuration = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    Iterator<String> keys = configuration.getKeys();
    List<String> keysToClear = new ArrayList<>();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE)
          || key.endsWith(V1Constants.MetadataKeys.Column.MAX_VALUE)
          || key.endsWith(V1Constants.MetadataKeys.Column.MIN_MAX_VALUE_INVALID)) {
        keysToClear.add(key);
      }
    }
    for (String key : keysToClear) {
      configuration.clearProperty(key);
    }
    SegmentMetadataUtils.savePropertiesConfiguration(configuration, indexDir);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
