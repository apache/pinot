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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator.UncompressedValueSizeTracking;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [SegmentDictionaryCreator#getTotalVariableLengthUncompressedValueSizeInBytes()].
///
/// Variable-width dictionary creators count each value's serialized bytes. Fixed-width types return `0` here because
/// their uncompressed value size is computed at segment-seal time. Multi-value columns count every element in every
/// row.
public class SegmentDictionaryCreatorUncompressedValueSizeTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      SegmentDictionaryCreatorUncompressedValueSizeTest.class.getSimpleName());

  @BeforeMethod
  public void setUp() throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  // -----------------------------------------------------------------------
  // STRING SV
  // -----------------------------------------------------------------------

  @Test
  public void testStringSvBasic() throws Exception {
    // "hello" and "world" each contain five UTF-8 bytes.
    String[] dict = {"hello", "world"};
    File dictFile = new File(TEMP_DIR, "stringSv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      // Simulate two rows, one with each value
      creator.indexOfSV("hello");
      creator.indexOfSV("world");

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 10L,
          "STRING SV: 'hello'(5) + 'world'(5) should give 10 UTF-8 bytes");
    }
  }

  @Test
  public void testStringSvMultiByteCharacters() throws Exception {
    // A four-character string with a two-byte UTF-8 final character contains five bytes.
    String cafeStr = "caf\u00e9";
    int expectedBytes = cafeStr.getBytes(StandardCharsets.UTF_8).length; // 5
    assertEquals(expectedBytes, 5, "The accented string should contain five UTF-8 bytes");

    String[] dict = {cafeStr, "hello"};
    File dictFile = new File(TEMP_DIR, "stringSvMultiByte.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      creator.indexOfSV(cafeStr);  // 5 UTF-8 bytes

      long total = creator.getTotalVariableLengthUncompressedValueSizeInBytes();
      assertEquals(total, 5L,
          "STRING SV: multi-byte char UTF-8 byte count (5) should be used, not Java char count (4)");
    }
  }

  @Test
  public void testStringSvRepeatedValues() throws Exception {
    // Same value indexed multiple times: each call to indexOfSV counts bytes
    String[] dict = {"foo"};
    File dictFile = new File(TEMP_DIR, "stringSvRepeat.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      Field cacheField = SegmentDictionaryCreator.class.getDeclaredField("_serializedValueLengthByDictId");
      cacheField.setAccessible(true);
      assertEquals((int[]) cacheField.get(creator), new int[]{3});

      creator.indexOfSV("foo");  // 3 bytes
      creator.indexOfSV("foo");  // 3 bytes
      creator.indexOfSV("foo");  // 3 bytes

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 9L,
          "STRING SV: three 'foo' rows (3 bytes each) should give 9 bytes total");
    }
  }

  @Test
  public void testMalformedSurrogateUsesDictionarySerializationLength()
      throws Exception {
    String malformed = "\uD800";
    String[] dict = {malformed};
    File dictFile = new File(TEMP_DIR, "malformedString.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);
      clearSerializedLengthCache(creator);
      creator.indexOfSV(malformed);

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(),
          malformed.getBytes(StandardCharsets.UTF_8).length,
          "Tracking must use the same malformed-input replacement behavior as dictionary serialization");
    }
  }

  // -----------------------------------------------------------------------
  // BYTES SV
  // -----------------------------------------------------------------------

  @Test
  public void testBytesSv() throws Exception {
    // Byte arrays of length five and three contribute eight total bytes.
    byte[] val5 = new byte[]{1, 2, 3, 4, 5};
    byte[] val3 = new byte[]{10, 20, 30};

    // Build a dictionary with ByteArray values sorted by value. SegmentDictionaryCreator.build() for BYTES
    // expects ByteArray[], so wrap the values
    ByteArray[] dict = {
        new ByteArray(val5),
        new ByteArray(val3)
    };
    // sort the dict (ByteArray is Comparable)
    Arrays.sort(dict);

    File dictFile = new File(TEMP_DIR, "bytesSv.dict");
    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.BYTES, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      // indexOfSV(byte[]) counts val.length per call
      creator.indexOfSV(val5);  // 5 bytes
      creator.indexOfSV(val3);  // 3 bytes

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 8L,
          "BYTES SV: byte array lengths 5+3 should give 8 uncompressed value bytes");
    }
  }

  @Test
  public void testBytesSvWithoutSerializedLengthCache() throws Exception {
    byte[] first = {1, 2, 3, 4, 5};
    byte[] second = {10, 20, 30};
    ByteArray[] dict = {new ByteArray(first), new ByteArray(second)};
    Arrays.sort(dict);

    try (SegmentDictionaryCreator creator = new SegmentDictionaryCreator("col", DataType.BYTES,
        new File(TEMP_DIR, "bytesSvNoCache.dict"), false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);
      clearSerializedLengthCache(creator);
      creator.indexOfSV(first);
      creator.indexOfSV(second);

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 8L);
    }
  }

  // -----------------------------------------------------------------------
  // STRING MV
  // -----------------------------------------------------------------------

  @Test
  public void testStringMv() throws Exception {
    // Row 1 contributes six bytes; row 2 contributes three bytes.
    // Total: 9 bytes
    String[] dict = {"bar", "baz", "foo"};  // sorted for dict build
    File dictFile = new File(TEMP_DIR, "stringMv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      // MV row 1: ["foo", "bar"]
      creator.indexOfMV(new String[]{"foo", "bar"});  // 3 + 3 = 6 bytes
      // MV row 2: ["baz"]
      creator.indexOfMV(new String[]{"baz"});          // 3 bytes

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 9L,
          "STRING MV: ['foo','bar'] + ['baz'] should give 9 UTF-8 bytes");
    }
  }

  @Test
  public void testGenericObjectMvUsesCachedDictionarySizes()
      throws Exception {
    String[] dict = {"a", "three"};
    File dictFile = new File(TEMP_DIR, "genericObjectMv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);
      creator.indexOfMV((Object) new Object[]{"three", "a", "three"});

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 11L);
    }
  }

  // -----------------------------------------------------------------------
  // Fixed-width INT SV values are counted at segment-seal time, so this creator returns zero.
  // -----------------------------------------------------------------------

  @Test
  public void testIntSvReturnsZero() throws Exception {
    // INT is a fixed-width type: bytes are counted at seal time from column stats, not here.
    int[] dict = {1, 2, 3, 100};
    File dictFile = new File(TEMP_DIR, "intSv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.INT, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      // Fixed-width dictionary lookups do not accumulate variable-width bytes.
      creator.indexOfSV(1);
      creator.indexOfSV(2);
      creator.indexOfSV(3);
      creator.indexOfSV(100);

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 0L,
          "INT SV: fixed-width type should return 0 from getTotalVariableLengthUncompressedValueSizeInBytes()");
    }
  }

  // -----------------------------------------------------------------------
  // BIG_DECIMAL SV
  // -----------------------------------------------------------------------

  @Test
  public void testBigDecimalSv() throws Exception {
    // BIG_DECIMAL uses BigDecimalUtils.serialize() byte length
    BigDecimal val1 = new BigDecimal("123.456");
    BigDecimal val2 = new BigDecimal("789.0");

    int val1Bytes = BigDecimalUtils.serialize(val1).length;
    int val2Bytes = BigDecimalUtils.serialize(val2).length;

    BigDecimal[] dict = {val1, val2};
    Arrays.sort(dict);  // BigDecimal is Comparable

    File dictFile = new File(TEMP_DIR, "bigDecimalSv.dict");
    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.BIG_DECIMAL, dictFile, false,
            UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      creator.indexOfSV((Object) val1);  // cast to Object to avoid ambiguity
      creator.indexOfSV((Object) val2);

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), (long) (val1Bytes + val2Bytes),
          "BIG_DECIMAL SV: should accumulate serialized byte lengths");
    }
  }

  @Test
  public void testBigDecimalSvWithoutSerializedLengthCache() throws Exception {
    BigDecimal first = new BigDecimal("123.456");
    BigDecimal second = new BigDecimal("789.0");
    BigDecimal[] dict = {first, second};
    Arrays.sort(dict);

    try (SegmentDictionaryCreator creator = new SegmentDictionaryCreator("col", DataType.BIG_DECIMAL,
        new File(TEMP_DIR, "bigDecimalSvNoCache.dict"), false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);
      clearSerializedLengthCache(creator);
      creator.indexOfSV((Object) first);
      creator.indexOfSV((Object) second);

      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(),
          (long) BigDecimalUtils.serialize(first).length + BigDecimalUtils.serialize(second).length);
    }
  }

  private static void clearSerializedLengthCache(SegmentDictionaryCreator creator)
      throws ReflectiveOperationException {
    Field cacheField = SegmentDictionaryCreator.class.getDeclaredField("_serializedValueLengthByDictId");
    cacheField.setAccessible(true);
    cacheField.set(creator, null);
  }

  // -----------------------------------------------------------------------
  // Initial state with no indexed rows.
  // -----------------------------------------------------------------------

  @Test
  public void testInitialStateIsZero() throws Exception {
    String[] dict = {"a", "b"};
    File dictFile = new File(TEMP_DIR, "initState.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, UncompressedValueSizeTracking.ENABLED)) {
      creator.build(dict);

      // No rows have been indexed yet.
      assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 0L,
          "getTotalVariableLengthUncompressedValueSizeInBytes() should be 0 before any rows are indexed");
    }
  }

  @Test
  public void testCloseKeepsTrackedTotal() throws Exception {
    SegmentDictionaryCreator creator = new SegmentDictionaryCreator("col", DataType.STRING,
        new File(TEMP_DIR, "cleanup.dict"), false, UncompressedValueSizeTracking.ENABLED);
    creator.build(new String[]{"hello", "world"});
    creator.indexOfSV("hello");

    creator.close();
    assertEquals(creator.getTotalVariableLengthUncompressedValueSizeInBytes(), 5L);
  }

  // -----------------------------------------------------------------------
  // End-to-end: dictionary.uncompressedValueSizeInBytes persisted in segment metadata.
  // -----------------------------------------------------------------------

  @Test
  public void testDictColumnUncompressedValueSizePersistsToSegmentMetadata() throws Exception {
    String tableName = "dictUncompressedValueTest";
    String col = "stringCol";
    File segDir = new File(TEMP_DIR, "seg");
    FileUtils.forceMkdir(segDir);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension(col, DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      GenericRow row = new GenericRow();
      row.putValue(col, "value_" + i);
      rows.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(segDir.getPath());
    config.setSegmentName("seg0");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(new File(segDir, "seg0"));
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(col);

    assertTrue(colMeta.hasDictionary(), col + " should be dict-encoded by default");
    assertTrue(colMeta.getDictionaryEncodedUncompressedValueSizeInBytes() > 0,
        "dictionary.uncompressedValueSizeInBytes should be > 0 after segment creation with compressionStatsEnabled");
  }

  @Test
  public void testFixedWidthMvDictColumnUncompressedValueSizePersistsElementCount()
      throws Exception {
    String tableName = "fixedWidthMvDictUncompressedValueTest";
    String col = "intMvCol";
    File segDir = new File(TEMP_DIR, "mvSeg");
    FileUtils.forceMkdir(segDir);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
        .addMultiValueDimension(col, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    Object[][] values = {
        {1, 2},
        {3, 4, 5},
        {6}
    };
    List<GenericRow> rows = new ArrayList<>();
    int totalValues = 0;
    for (Object[] rowValues : values) {
      GenericRow row = new GenericRow();
      row.putValue(col, rowValues);
      rows.add(row);
      totalValues += rowValues.length;
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(segDir.getPath());
    config.setSegmentName("mvSeg0");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(new File(segDir, "mvSeg0"));
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(col);

    assertTrue(colMeta.hasDictionary(), col + " should be dict-encoded by default");
    assertEquals(colMeta.getDictionaryEncodedUncompressedValueSizeInBytes(), (long) totalValues * Integer.BYTES,
        "Fixed-width MV dict stats should count every element, not just every document");
  }
}
