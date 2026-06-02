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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link SegmentDictionaryCreator#getTotalRawIngestBytes()}.
 *
 * <p>The method accumulates raw ingest bytes only for variable-length types (STRING, BYTES, BIG_DECIMAL).
 * Fixed-width types (INT, LONG, FLOAT, DOUBLE) return {@code 0} from {@code getTotalRawIngestBytes()}
 * because their sizes are counted at segment-seal time from the column statistics.
 *
 * <p>Multi-value (MV) columns accumulate bytes across all elements in every row.
 */
public class SegmentDictionaryCreatorRawIngestSizeTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      SegmentDictionaryCreatorRawIngestSizeTest.class.getSimpleName());

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
    // "hello" = 5 UTF-8 bytes, "world" = 5 UTF-8 bytes → 10 total
    String[] dict = {"hello", "world"};
    File dictFile = new File(TEMP_DIR, "stringSv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, true)) {
      creator.build(dict);

      // Simulate two rows, one with each value
      creator.indexOfSV("hello");
      creator.indexOfSV("world");

      assertEquals(creator.getTotalRawIngestBytes(), 10L,
          "STRING SV: 'hello'(5) + 'world'(5) should give 10 UTF-8 bytes");
    }
  }

  @Test
  public void testStringSvMultiByteCharacters() throws Exception {
    // "café" = 5 UTF-8 bytes (c=1, a=1, f=1, é=2) but 4 Java chars
    // Verifies that byte count is used, not char count.
    String cafeStr = "café"; // café
    int expectedBytes = cafeStr.getBytes(StandardCharsets.UTF_8).length; // 5
    assertEquals(expectedBytes, 5, "Sanity: café should be 5 UTF-8 bytes");

    String[] dict = {cafeStr, "hello"};
    File dictFile = new File(TEMP_DIR, "stringSvMultiByte.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, true)) {
      creator.build(dict);

      creator.indexOfSV(cafeStr);  // 5 UTF-8 bytes

      long total = creator.getTotalRawIngestBytes();
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
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, true)) {
      creator.build(dict);

      creator.indexOfSV("foo");  // 3 bytes
      creator.indexOfSV("foo");  // 3 bytes
      creator.indexOfSV("foo");  // 3 bytes

      assertEquals(creator.getTotalRawIngestBytes(), 9L,
          "STRING SV: three 'foo' rows (3 bytes each) should give 9 bytes total");
    }
  }

  // -----------------------------------------------------------------------
  // BYTES SV
  // -----------------------------------------------------------------------

  @Test
  public void testBytesSv() throws Exception {
    // byte[] of length 5 and 3 → expect 8 total
    byte[] val5 = new byte[]{1, 2, 3, 4, 5};
    byte[] val3 = new byte[]{10, 20, 30};

    // Build dict with ByteArray (sorted by value) — the SegmentDictionaryCreator.build() for BYTES
    // expects ByteArray[], so wrap the values
    org.apache.pinot.spi.utils.ByteArray[] dict = {
        new org.apache.pinot.spi.utils.ByteArray(val5),
        new org.apache.pinot.spi.utils.ByteArray(val3)
    };
    // sort the dict (ByteArray is Comparable)
    java.util.Arrays.sort(dict);

    File dictFile = new File(TEMP_DIR, "bytesSv.dict");
    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.BYTES, dictFile, false, true)) {
      creator.build(dict);

      // indexOfSV(byte[]) counts val.length per call
      creator.indexOfSV(val5);  // 5 bytes
      creator.indexOfSV(val3);  // 3 bytes

      assertEquals(creator.getTotalRawIngestBytes(), 8L,
          "BYTES SV: byte array lengths 5+3 should give 8 raw ingest bytes");
    }
  }

  // -----------------------------------------------------------------------
  // STRING MV
  // -----------------------------------------------------------------------

  @Test
  public void testStringMv() throws Exception {
    // Row 1: ["foo", "bar"] → 3+3 = 6 bytes
    // Row 2: ["baz"] → 3 bytes
    // Total: 9 bytes
    String[] dict = {"bar", "baz", "foo"};  // sorted for dict build
    File dictFile = new File(TEMP_DIR, "stringMv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, true)) {
      creator.build(dict);

      // MV row 1: ["foo", "bar"]
      creator.indexOfMV(new String[]{"foo", "bar"});  // 3 + 3 = 6 bytes
      // MV row 2: ["baz"]
      creator.indexOfMV(new String[]{"baz"});          // 3 bytes

      assertEquals(creator.getTotalRawIngestBytes(), 9L,
          "STRING MV: ['foo','bar'] + ['baz'] should give 9 UTF-8 bytes");
    }
  }

  // -----------------------------------------------------------------------
  // INT SV — fixed-width type should return 0
  // -----------------------------------------------------------------------

  @Test
  public void testIntSvReturnsZero() throws Exception {
    // INT is a fixed-width type: bytes are counted at seal time from column stats, not here.
    int[] dict = {1, 2, 3, 100};
    File dictFile = new File(TEMP_DIR, "intSv.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.INT, dictFile, false, true)) {
      creator.build(dict);

      // Call indexOfSV for each value — should NOT accumulate any bytes
      creator.indexOfSV(1);
      creator.indexOfSV(2);
      creator.indexOfSV(3);
      creator.indexOfSV(100);

      assertEquals(creator.getTotalRawIngestBytes(), 0L,
          "INT SV: fixed-width type should return 0 from getTotalRawIngestBytes()");
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
    java.util.Arrays.sort(dict);  // BigDecimal is Comparable

    File dictFile = new File(TEMP_DIR, "bigDecimalSv.dict");
    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.BIG_DECIMAL, dictFile, false, true)) {
      creator.build(dict);

      creator.indexOfSV((Object) val1);  // cast to Object to avoid ambiguity
      creator.indexOfSV((Object) val2);

      assertEquals(creator.getTotalRawIngestBytes(), (long) (val1Bytes + val2Bytes),
          "BIG_DECIMAL SV: should accumulate serialized byte lengths");
    }
  }

  // -----------------------------------------------------------------------
  // Initial state — no rows indexed
  // -----------------------------------------------------------------------

  @Test
  public void testInitialStateIsZero() throws Exception {
    String[] dict = {"a", "b"};
    File dictFile = new File(TEMP_DIR, "initState.dict");

    try (SegmentDictionaryCreator creator =
        new SegmentDictionaryCreator("col", DataType.STRING, dictFile, false, true)) {
      creator.build(dict);

      // No rows indexed yet — should be 0
      assertEquals(creator.getTotalRawIngestBytes(), 0L,
          "getTotalRawIngestBytes() should be 0 before any rows are indexed");
    }
  }
}
