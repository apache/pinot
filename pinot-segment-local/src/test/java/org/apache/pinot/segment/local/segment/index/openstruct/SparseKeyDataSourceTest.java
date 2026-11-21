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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SparseKeyDataSourceTest {
  // Doc 0 has every key; doc 1 is an empty blob; doc 2 misses these keys but has another.
  private static final String[] BLOBS = {
      "{\"i\":7,\"l\":123456789012,\"f\":1.5,\"d\":2.25,\"s\":\"hello\",\"bd\":3.14,\"n\":42,"
          + "\"b\":\"aGVsbG8=\",\"jn\":null,\"obj\":{\"os\":\"android\",\"sdk\":33},"
          + "\"arr\":[1,2,3]}",
      null,
      "{\"other\":1}",
  };

  /// Numeric text converts to a number and fractions truncate for integral keys. Nonnumeric text, objects and
  /// arrays are absent for these single-value numeric keys.
  private static final String[] MIXED = {
      "{\"k\":5}", "{\"k\":\"abc\"}", "{\"k\":1.5}", "{\"k\":2.0}", "{\"k\":\"7\"}", "{\"k\":{\"x\":1}}",
      "{\"k\":true}", "{\"k\":[1,2]}", "{\"k\":9223372036854775807}",
  };

  private static SparseKeyDataSource of(DataType declaredType, String... blobs) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(blobs), FakeStringForwardIndex.nullVector(blobs), blobs.length);
    return new SparseKeyDataSource(new DimensionFieldSpec("k", declaredType, true), blob, 0);
  }

  /// A multi-value key converts each element as the dense tier does; when any element is rejected the dense tier drops
  /// the whole value (its length is part of it), so the document reads as absent.
  @Test
  public void testMultiValueKeyIsAbsentWhenAnyElementIsRejected() {
    String[] blobs = {"{\"k\":[1,\"2\"]}", "{\"k\":[1,2,\"abc\"]}", "{\"k\":7}", "{\"k\":[]}"};
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs),
        FakeStringForwardIndex.nullVector(blobs), blobs.length);
    DimensionFieldSpec spec = new DimensionFieldSpec("k", DataType.LONG, false);
    spec.setDefaultNullValue(-7L);
    SparseKeyDataSource longs = new SparseKeyDataSource(spec, blob, 2);
    ForwardIndexReader<?> fwd = longs.getForwardIndex();
    assertEquals(fwd.getLongMV(0, null), new long[]{1, 2});
    assertEquals(fwd.getNumValuesMV(1, null), 1);
    long[] buffer = new long[1];
    assertEquals(fwd.getLongMV(1, buffer, null), 1);
    assertEquals(buffer[0], -7L);
    assertEquals(fwd.getLongMV(1, null), new long[]{-7L});
    assertEquals(fwd.getLongMV(2, null), new long[]{7});
    assertFalse(longs.getNullValueVector().isNull(0));
    assertTrue(longs.getNullValueVector().isNull(1));
    assertFalse(longs.getNullValueVector().isNull(2));
    assertTrue(longs.getNullValueVector().isNull(3));
    assertEquals(fwd.getNumValuesMV(3, null), 1);
    assertEquals(fwd.getLongMV(3, null), new long[]{-7L});
  }

  private static SparseKeyDataSource multiValue(DataType declaredType, String... blobs) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs),
        FakeStringForwardIndex.nullVector(blobs), blobs.length);
    return new SparseKeyDataSource(new DimensionFieldSpec("k", declaredType, false), blob, 3);
  }

  /// Every multi-value getter converts each element to the key's stored type (numeric text and fractions as the dense
  /// tier converts them), reads a document without the key as the single default the dense tier stores for it, reads a
  /// scalar stored before the key became multi-value as a one-element value, and fills a caller's buffer.
  @Test
  public void testMultiValueNumericGettersConvertEachElement() {
    String[] blobs = {"{\"k\":[1,\"2\",3.0]}", null, "{\"k\":4}"};

    ForwardIndexReader<?> ints = multiValue(DataType.INT, blobs).getForwardIndex();
    assertEquals(ints.getNumValuesMV(0, null), 3);
    assertEquals(ints.getIntMV(0, null), new int[]{1, 2, 3});
    int[] intBuffer = new int[3];
    assertEquals(ints.getIntMV(0, intBuffer, null), 3);
    assertEquals(intBuffer, new int[]{1, 2, 3});
    assertEquals(ints.getIntMV(1, null), new int[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
    assertEquals(ints.getNumValuesMV(1, null), 1);
    assertEquals(ints.getIntMV(2, null), new int[]{4});

    ForwardIndexReader<?> longs = multiValue(DataType.LONG, blobs).getForwardIndex();
    long[] longBuffer = new long[3];
    assertEquals(longs.getLongMV(0, longBuffer, null), 3);
    assertEquals(longBuffer, new long[]{1, 2, 3});

    ForwardIndexReader<?> floats = multiValue(DataType.FLOAT, blobs).getForwardIndex();
    assertEquals(floats.getFloatMV(0, null), new float[]{1f, 2f, 3f});
    float[] floatBuffer = new float[3];
    assertEquals(floats.getFloatMV(0, floatBuffer, null), 3);
    assertEquals(floatBuffer, new float[]{1f, 2f, 3f});
    assertEquals(floats.getFloatMV(1, null), new float[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
    assertEquals(floats.getFloatMV(2, null), new float[]{4f});

    ForwardIndexReader<?> doubles = multiValue(DataType.DOUBLE, blobs).getForwardIndex();
    assertEquals(doubles.getDoubleMV(0, null), new double[]{1.0, 2.0, 3.0});
    double[] doubleBuffer = new double[3];
    assertEquals(doubles.getDoubleMV(0, doubleBuffer, null), 3);
    assertEquals(doubleBuffer, new double[]{1.0, 2.0, 3.0});
    assertEquals(doubles.getDoubleMV(1, null), new double[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
    assertEquals(doubles.getDoubleMV(2, null), new double[]{4.0});
  }

  @Test
  public void testMultiValueBigDecimalGettersConvertEachElement() {
    String[] blobs = {"{\"k\":[1,\"2.5\",3.25,9223372036854775809]}", null, "{\"k\":4}"};
    ForwardIndexReader<?> decimals = multiValue(DataType.BIG_DECIMAL, blobs).getForwardIndex();

    BigDecimal beyondLong = new BigDecimal("9223372036854775809");
    BigDecimal[] values = decimals.getBigDecimalMV(0, null);
    assertEquals(values.length, 4);
    assertEquals(values[0].compareTo(new BigDecimal("1")), 0);
    assertEquals(values[1].compareTo(new BigDecimal("2.5")), 0);
    assertEquals(values[2].compareTo(new BigDecimal("3.25")), 0);
    assertEquals(values[3].compareTo(beyondLong), 0);
    BigDecimal[] buffer = new BigDecimal[4];
    assertEquals(decimals.getBigDecimalMV(0, buffer, null), 4);
    assertEquals(buffer[1].compareTo(new BigDecimal("2.5")), 0);
    assertEquals(buffer[3].compareTo(beyondLong), 0);
    assertEquals(decimals.getBigDecimalMV(1, null),
        new BigDecimal[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL});
    assertEquals(decimals.getBigDecimalMV(2, null)[0].compareTo(new BigDecimal("4")), 0);
  }

  /// A STRING multi-value key takes every element as text (a nested element as JSON, not the empty string), and a
  /// BYTES one decodes each base64 element, reading a malformed or non-binary element as the default.
  @Test
  public void testMultiValueStringAndBytesGetters() {
    String[] stringBlobs = {"{\"k\":[\"a\",{\"x\":1},5,true]}", "{\"k\":[]}", "{\"k\":\"s\"}"};
    SparseKeyDataSource stringSource = multiValue(DataType.STRING, stringBlobs);
    ForwardIndexReader<?> strings = stringSource.getForwardIndex();
    String[] expected = {"a", "{\"x\":1}", "5", "true"};
    assertEquals(strings.getNumValuesMV(0, null), 4);
    assertEquals(strings.getStringMV(0, null), expected);
    String[] stringBuffer = new String[4];
    assertEquals(strings.getStringMV(0, stringBuffer, null), 4);
    assertEquals(stringBuffer, expected);
    assertTrue(stringSource.getNullValueVector().isNull(1));
    assertEquals(strings.getNumValuesMV(1, null), 1);
    assertEquals(strings.getStringMV(1, null), new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    assertEquals(strings.getStringMV(2, null), new String[]{"s"});

    String[] bytesBlobs = {"{\"k\":[\"aGVsbG8=\",\"!!not base64\",5]}", null};
    ForwardIndexReader<?> bytes = multiValue(DataType.BYTES, bytesBlobs).getForwardIndex();
    byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] absent = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
    assertEquals(bytes.getBytesMV(0, null), new byte[][]{hello, absent, absent});
    byte[][] bytesBuffer = new byte[3][];
    assertEquals(bytes.getBytesMV(0, bytesBuffer, null), 3);
    assertEquals(bytesBuffer, new byte[][]{hello, absent, absent});
    assertEquals(bytes.getBytesMV(1, null), new byte[][]{absent});
  }

  /// A null element takes the whole multi-value out, as the dense tier drops it (length is part of the value), and the
  /// null vector agrees.
  @Test
  public void testMultiValueKeyWithANullElementIsAbsent() {
    SparseKeyDataSource longs = multiValue(DataType.LONG, "{\"k\":[1,null]}", "{\"k\":[1,2]}");
    assertTrue(longs.getNullValueVector().isNull(0));
    assertFalse(longs.getNullValueVector().isNull(1));
    assertEquals(longs.getForwardIndex().getLongMV(0, null),
        new long[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
    assertEquals(longs.getForwardIndex().getLongMV(1, null), new long[]{1, 2});

    String withNull = "{\"k\":[\"aGVsbG8=\",null]}";
    SparseKeyDataSource strings = multiValue(DataType.STRING, withNull);
    assertTrue(strings.getNullValueVector().isNull(0));
    assertEquals(strings.getForwardIndex().getNumValuesMV(0, null), 1);
    assertEquals(strings.getForwardIndex().getStringMV(0, null),
        new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    SparseKeyDataSource bytes = multiValue(DataType.BYTES, withNull);
    assertTrue(bytes.getNullValueVector().isNull(0));
    assertEquals(bytes.getForwardIndex().getBytesMV(0, null),
        new byte[][]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES});
  }

  @Test
  public void testExponentOutsideDecimalRange() {
    for (String token : new String[]{"1e-2147483649", "1e2147483649"}) {
      String blob = "{\"k\":1,\"other\":" + token + "}";
      assertEquals(of(DataType.LONG, blob).getForwardIndex().getLong(0, null), 1L);
      assertEquals(of(DataType.BIG_DECIMAL, blob).getForwardIndex().getBigDecimal(0, null), BigDecimal.ONE);
      SparseKeyDataSource missing = of(DataType.BIG_DECIMAL, "{\"k\":" + token + "}");
      assertTrue(missing.getNullValueVector().isNull(0));
      assertEquals(missing.getForwardIndex().getBigDecimal(0, null),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    }
  }

  @Test
  public void testIntOverflowFromTextIsAbsent() {
    SparseKeyDataSource ints = of(DataType.INT, "{\"k\":\"2147483648\"}", "{\"k\":\"42\"}");
    assertTrue(ints.getNullValueVector().isNull(0));
    assertEquals(ints.getForwardIndex().getInt(1, null), 42);
  }

  private static SparseKeyDataSource mixed(DataType declaredType) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(MIXED), FakeStringForwardIndex.nullVector(MIXED), MIXED.length);
    return new SparseKeyDataSource(new DimensionFieldSpec("k", declaredType, true), blob, 0);
  }

  @Test
  public void testDeclaredNumericKeyReadsOnlyValuesThatFit() {
    // Text that is not a number, an object and an array read as absent rather than as an invented 0, and the null
    // vector agrees; a fraction converts as the dense tier converts it. OpenStructSparseDenseParityTest checks
    // every type against the dense tier itself.
    SparseKeyDataSource longs = mixed(DataType.LONG);
    ForwardIndexReader<?> fwd = longs.getForwardIndex();
    long absent = (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG;
    for (int doc : new int[]{1, 5, 7}) {
      assertEquals(fwd.getLong(doc, null), absent, MIXED[doc]);
      assertTrue(longs.getNullValueVector().isNull(doc), MIXED[doc]);
    }
    assertEquals(fwd.getLong(0, null), 5L);
    assertEquals(fwd.getLong(2, null), 1L);
    assertEquals(fwd.getLong(4, null), 7L);
    assertEquals(fwd.getLong(8, null), Long.MAX_VALUE);
    assertFalse(longs.getNullValueVector().isNull(2));

    SparseKeyDataSource doubles = mixed(DataType.DOUBLE);
    assertEquals(doubles.getForwardIndex().getDouble(2, null), 1.5);
    assertEquals(doubles.getForwardIndex().getDouble(4, null), 7.0);
    assertTrue(doubles.getNullValueVector().isNull(1));
    assertTrue(doubles.getNullValueVector().isNull(5));
    assertFalse(doubles.getNullValueVector().isNull(2));

    // STRING still takes every value.
    SparseKeyDataSource strings = mixed(DataType.STRING);
    for (int doc = 0; doc < MIXED.length; doc++) {
      assertFalse(strings.getNullValueVector().isNull(doc), MIXED[doc]);
    }
  }

  private static SparseKeyDataSource source(String key, DataType declaredType) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(BLOBS), FakeStringForwardIndex.nullVector(BLOBS), BLOBS.length);
    return new SparseKeyDataSource(new DimensionFieldSpec(key, declaredType, true), blob, 0);
  }

  @Test
  public void testDeclaredTypesRoundTripAndDefaultWhenAbsent() {
    SparseKeyDataSource intSrc = source("i", DataType.INT);
    ForwardIndexReader<?> fwd = intSrc.getForwardIndex();
    assertEquals(fwd.getInt(0, null), 7);
    assertEquals(fwd.getInt(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(fwd.getInt(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);

    SparseKeyDataSource longSrc = source("l", DataType.LONG);
    fwd = longSrc.getForwardIndex();
    assertEquals(fwd.getLong(0, null), 123456789012L);
    assertEquals(fwd.getLong(1, null), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(fwd.getLong(2, null), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);

    SparseKeyDataSource floatSrc = source("f", DataType.FLOAT);
    fwd = floatSrc.getForwardIndex();
    assertEquals(fwd.getFloat(0, null), 1.5f);
    assertEquals(fwd.getFloat(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    assertEquals(fwd.getFloat(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);

    SparseKeyDataSource doubleSrc = source("d", DataType.DOUBLE);
    fwd = doubleSrc.getForwardIndex();
    assertEquals(fwd.getDouble(0, null), 2.25);
    assertEquals(fwd.getDouble(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    assertEquals(fwd.getDouble(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);

    SparseKeyDataSource strSrc = source("s", DataType.STRING);
    fwd = strSrc.getForwardIndex();
    assertEquals(fwd.getString(0, null), "hello");
    assertEquals(fwd.getString(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(fwd.getString(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);

    SparseKeyDataSource bdSrc = source("bd", DataType.BIG_DECIMAL);
    fwd = bdSrc.getForwardIndex();
    assertEquals(fwd.getBigDecimal(0, null), new BigDecimal("3.14"));
    assertEquals(fwd.getBigDecimal(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    assertEquals(fwd.getBigDecimal(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
  }

  @Test
  public void testContainerValuesSerializeAsJsonRatherThanEmptyString() {
    // asText() is the empty string for an object or array node, which made a nested value indistinguishable from a
    // missing key. A blob key holding a document must read back as that document.
    assertEquals(source("obj", DataType.STRING).getForwardIndex().getString(0, null),
        "{\"os\":\"android\",\"sdk\":33}");
    assertEquals(source("arr", DataType.STRING).getForwardIndex().getString(0, null), "[1,2,3]");

    // Scalars and absent keys are untouched.
    assertEquals(source("s", DataType.STRING).getForwardIndex().getString(0, null), "hello");
    assertEquals(source("obj", DataType.STRING).getForwardIndex().getString(2, null),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
  }

  @Test
  public void testAbsentKeyReadsAsTheDeclaredDefaultNotTheTypeDefault() {
    // A key absent from the segment entirely already reads as the declared default, through
    // OpenStructDataSource.getValueFieldSpec. A key absent from one document should not read as something else --
    // and MapFilterOperator refuses the JSON-index fast path for exactly the declared default, so the two
    // disagreeing let a NOT_IN over that value take the fast path and miss the documents that lack the key.
    DimensionFieldSpec spec = new DimensionFieldSpec("i", DataType.INT, true, -7);
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(BLOBS), FakeStringForwardIndex.nullVector(BLOBS), BLOBS.length);
    ForwardIndexReader<?> fwd = new SparseKeyDataSource(spec, blob, 0).getForwardIndex();

    assertEquals(fwd.getInt(0, null), 7);
    // Doc 2 has other keys but not this one.
    assertEquals(fwd.getInt(2, null), -7);
  }

  @Test
  public void testAbsentKeyStillFallsBackToTheTypeDefaultWhenNoneIsDeclared() {
    assertEquals(source("i", DataType.INT).getForwardIndex().getInt(2, null),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
  }

  @Test
  public void testUndeclaredKeyReadsAsString() {
    SparseKeyDataSource src = source("n", DataType.STRING);
    assertEquals(src.getForwardIndex().getString(0, null), "42");
  }

  @Test
  public void testNullVectorMarksDocsMissingTheKey() {
    SparseKeyDataSource src = source("i", DataType.INT);
    NullValueVectorReader nv = src.getNullValueVector();
    assertNotNull(nv);
    ImmutableRoaringBitmap bm = nv.getNullBitmap();
    assertFalse(bm.contains(0));  // doc 0 has "i"
    assertTrue(bm.contains(1));   // doc 1 is null blob
    assertTrue(bm.contains(2));   // doc 2 has "other", not "i"
  }

  @Test
  public void testMetadataShape() {
    SparseKeyDataSource src = source("i", DataType.INT);
    DataSourceMetadata meta = src.getDataSourceMetadata();
    assertEquals(meta.getNumDocs(), 3);
    assertEquals(meta.getFieldSpec().getDataType(), DataType.INT);
    assertTrue(meta.getFieldSpec().isSingleValueField());
    assertNull(src.getDictionary());
    assertEquals(meta.getCardinality(), Constants.UNKNOWN_CARDINALITY);
  }

  @Test
  public void testBytesRoundTripAndDefaultWhenAbsent() {
    // "aGVsbG8=" is base64 for "hello"
    SparseKeyDataSource src = source("b", DataType.BYTES);
    ForwardIndexReader<?> fwd = src.getForwardIndex();
    assertEquals(fwd.getBytes(0, null), "hello".getBytes(StandardCharsets.UTF_8));
    assertEquals(fwd.getBytes(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    assertEquals(fwd.getBytes(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
  }

  @Test
  public void testBytesFallsBackToDefaultForNonBinaryNodes() {
    // Key "n" is an int node: binaryValue() returns null. Key "s" is "hello", which is not valid base64:
    // TextNode.binaryValue() throws. Both must fold to the default rather than failing the query.
    assertEquals(source("n", DataType.BYTES).getForwardIndex().getBytes(0, null),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    assertEquals(source("s", DataType.BYTES).getForwardIndex().getBytes(0, null),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
  }

  @Test
  public void testExplicitJsonNullTreatedAsAbsent() {
    // Key "jn" is present in doc 0 as explicit JSON null — forward reader and null vector
    // must agree: forward returns the default, null vector marks it as null.
    SparseKeyDataSource src = source("jn", DataType.INT);
    ForwardIndexReader<?> fwd = src.getForwardIndex();
    assertEquals(fwd.getInt(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);

    NullValueVectorReader nv = src.getNullValueVector();
    assertTrue(nv.isNull(0));
    assertTrue(nv.isNull(1));
    assertTrue(nv.isNull(2));
  }
}
