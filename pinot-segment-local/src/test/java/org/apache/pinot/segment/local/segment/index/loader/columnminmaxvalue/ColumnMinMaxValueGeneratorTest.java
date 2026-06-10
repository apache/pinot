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
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Regression tests for {@link ColumnMinMaxValueGenerator} on raw (no-dictionary) BYTES and UUID columns.
 *
 * <p>The raw-BYTES loop once had its comparison directions inverted ({@code > 0} accepted as new min,
 * {@code < 0} as new max), silently persisting swapped min/max metadata that value-based segment pruning then
 * consumed. These tests build a real segment, strip the min/max metadata the segment-build stats collector wrote,
 * re-generate via the loader path, and assert the persisted direction.
 */
public class ColumnMinMaxValueGeneratorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ColumnMinMaxValueGeneratorTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "testSegment";
  private static final String BYTES_COLUMN = "bytesCol";
  private static final String BYTES_MV_COLUMN = "bytesMvCol";
  private static final String UUID_COLUMN = "uuidCol";

  // Ordered ascending by unsigned byte-wise comparison
  private static final byte[] BYTES_SMALL = new byte[]{0x00, 0x01};
  private static final byte[] BYTES_MID = new byte[]{0x10, (byte) 0xff};
  private static final byte[] BYTES_LARGE = new byte[]{(byte) 0x80, 0x00};

  private static final String UUID_SMALL = "00000000-0000-0000-0000-000000000001";
  private static final String UUID_MID = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_LARGE = "ffffffff-ffff-ffff-ffff-fffffffffffe";

  @Test
  public void testRawBytesAndUuidMinMaxDirection()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
        .addMultiValueDimension(BYTES_MV_COLUMN, DataType.BYTES)
        .addSingleValueDimension(UUID_COLUMN, DataType.UUID)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(List.of(BYTES_COLUMN, BYTES_MV_COLUMN, UUID_COLUMN))
        .build();

    File indexDir = buildSegment(tableConfig, schema);
    removeMinMaxValuesFromMetadataFile(indexDir);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ColumnMinMaxValueGenerator generator =
          new ColumnMinMaxValueGenerator(segmentMetadata, writer, ColumnMinMaxValueGeneratorMode.ALL);
      generator.addColumnMinMaxValue();
    }

    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(indexDir);

    Comparable<?> bytesMin = reloaded.getColumnMetadataFor(BYTES_COLUMN).getMinValue();
    Comparable<?> bytesMax = reloaded.getColumnMetadataFor(BYTES_COLUMN).getMaxValue();
    assertNotNull(bytesMin);
    assertNotNull(bytesMax);
    assertEquals(bytesMin, new ByteArray(BYTES_SMALL), "Raw BYTES min must be the smallest unsigned value");
    assertEquals(bytesMax, new ByteArray(BYTES_LARGE), "Raw BYTES max must be the largest unsigned value");
    assertTrue(((ByteArray) bytesMin).compareTo((ByteArray) bytesMax) < 0, "min must order before max");

    Comparable<?> uuidMin = reloaded.getColumnMetadataFor(UUID_COLUMN).getMinValue();
    Comparable<?> uuidMax = reloaded.getColumnMetadataFor(UUID_COLUMN).getMaxValue();
    assertNotNull(uuidMin);
    assertNotNull(uuidMax);
    assertEquals(uuidMin, new ByteArray(UuidUtils.toBytes(UUID_SMALL)),
        "UUID min must be the smallest canonical UUID");
    assertEquals(uuidMax, new ByteArray(UuidUtils.toBytes(UUID_LARGE)),
        "UUID max must be the largest canonical UUID");

    // The raw-BYTES MV loop is a distinct code path sharing the comparator with the SV loop
    Comparable<?> bytesMvMin = reloaded.getColumnMetadataFor(BYTES_MV_COLUMN).getMinValue();
    Comparable<?> bytesMvMax = reloaded.getColumnMetadataFor(BYTES_MV_COLUMN).getMaxValue();
    assertNotNull(bytesMvMin);
    assertNotNull(bytesMvMax);
    assertEquals(bytesMvMin, new ByteArray(BYTES_SMALL), "Raw MV BYTES min must be the smallest unsigned value");
    assertEquals(bytesMvMax, new ByteArray(BYTES_LARGE), "Raw MV BYTES max must be the largest unsigned value");
  }

  private static File buildSegment(TableConfig tableConfig, Schema schema)
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>();
    // Deliberately insert in non-sorted order so the running min/max comparisons are both exercised
    for (Object[] values : new Object[][]{
        {BYTES_MID, UuidUtils.toBytes(UUID_MID)},
        {BYTES_LARGE, UuidUtils.toBytes(UUID_LARGE)},
        {BYTES_SMALL, UuidUtils.toBytes(UUID_SMALL)}
    }) {
      GenericRow row = new GenericRow();
      row.putValue(BYTES_COLUMN, values[0]);
      // Each MV row carries two values so the inner per-value loop is exercised as well
      row.putValue(BYTES_MV_COLUMN, new Object[]{values[0], BYTES_MID});
      row.putValue(UUID_COLUMN, values[1]);
      rows.add(row);
    }

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
