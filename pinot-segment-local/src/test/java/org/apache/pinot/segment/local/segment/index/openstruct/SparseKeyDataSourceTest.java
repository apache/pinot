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
          + "\"b\":\"aGVsbG8=\",\"jn\":null}",
      null,
      "{\"other\":1}",
  };

  private static SparseKeyDataSource source(String key, DataType declaredType) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(BLOBS), FakeStringForwardIndex.nullVector(BLOBS), BLOBS.length);
    return new SparseKeyDataSource(new DimensionFieldSpec(key, declaredType, true), blob);
  }

  @Test
  public void testDeclaredTypesRoundTripAndDefaultWhenAbsent() {
    // INT
    SparseKeyDataSource intSrc = source("i", DataType.INT);
    ForwardIndexReader<?> fwd = intSrc.getForwardIndex();
    assertEquals(fwd.getInt(0, null), 7);
    assertEquals(fwd.getInt(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(fwd.getInt(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);

    // LONG
    SparseKeyDataSource longSrc = source("l", DataType.LONG);
    fwd = longSrc.getForwardIndex();
    assertEquals(fwd.getLong(0, null), 123456789012L);
    assertEquals(fwd.getLong(1, null), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(fwd.getLong(2, null), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);

    // FLOAT
    SparseKeyDataSource floatSrc = source("f", DataType.FLOAT);
    fwd = floatSrc.getForwardIndex();
    assertEquals(fwd.getFloat(0, null), 1.5f);
    assertEquals(fwd.getFloat(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    assertEquals(fwd.getFloat(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);

    // DOUBLE
    SparseKeyDataSource doubleSrc = source("d", DataType.DOUBLE);
    fwd = doubleSrc.getForwardIndex();
    assertEquals(fwd.getDouble(0, null), 2.25);
    assertEquals(fwd.getDouble(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    assertEquals(fwd.getDouble(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);

    // STRING
    SparseKeyDataSource strSrc = source("s", DataType.STRING);
    fwd = strSrc.getForwardIndex();
    assertEquals(fwd.getString(0, null), "hello");
    assertEquals(fwd.getString(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(fwd.getString(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);

    // BIG_DECIMAL
    SparseKeyDataSource bdSrc = source("bd", DataType.BIG_DECIMAL);
    fwd = bdSrc.getForwardIndex();
    assertEquals(fwd.getBigDecimal(0, null), new BigDecimal("3.14"));
    assertEquals(fwd.getBigDecimal(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    assertEquals(fwd.getBigDecimal(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
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
    assertEquals(fwd.getBytes(0, null), "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    assertEquals(fwd.getBytes(1, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    assertEquals(fwd.getBytes(2, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
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
