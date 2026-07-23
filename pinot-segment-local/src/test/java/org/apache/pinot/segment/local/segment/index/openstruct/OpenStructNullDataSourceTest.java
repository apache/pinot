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

import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class OpenStructNullDataSourceTest {

  @Test
  public void testMetadataReportsCorrectTypeAndNumDocs() {
    FieldSpec spec = new DimensionFieldSpec("region", FieldSpec.DataType.STRING, true);
    OpenStructNullDataSource ds = new OpenStructNullDataSource(spec, 100);
    DataSourceMetadata metadata = ds.getDataSourceMetadata();

    assertEquals(metadata.getFieldSpec().getDataType(), FieldSpec.DataType.STRING);
    assertEquals(metadata.getNumDocs(), 100);
    assertEquals(metadata.getCardinality(), 1);
  }

  @Test
  public void testNullValueVectorAllDocsNull() {
    OpenStructNullDataSource ds = new OpenStructNullDataSource(
        new DimensionFieldSpec("k", FieldSpec.DataType.INT, true), 50);
    NullValueVectorReader nullReader = ds.getNullValueVector();

    assertNotNull(nullReader);
    for (int i = 0; i < 50; i++) {
      assertTrue(nullReader.isNull(i));
    }
    ImmutableRoaringBitmap bitmap = nullReader.getNullBitmap();
    assertEquals(bitmap.getCardinality(), 50);
  }

  @Test
  public void testNullValueVectorZeroDocs() {
    OpenStructNullDataSource ds = new OpenStructNullDataSource(
        new DimensionFieldSpec("k", FieldSpec.DataType.INT, true), 0);
    NullValueVectorReader nullReader = ds.getNullValueVector();
    assertEquals(nullReader.getNullBitmap().getCardinality(), 0);
  }

  @Test
  public void testForwardIndexReturnsDefaultNullValues() {
    // INT
    verifyForwardIndex(FieldSpec.DataType.INT, 10, (fwd) -> {
      assertEquals(fwd.getInt(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(fwd.getStoredType(), FieldSpec.DataType.INT);
    });
    // LONG
    verifyForwardIndex(FieldSpec.DataType.LONG, 10, (fwd) -> {
      assertEquals(fwd.getLong(0, null), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    });
    // FLOAT
    verifyForwardIndex(FieldSpec.DataType.FLOAT, 10, (fwd) -> {
      assertEquals(fwd.getFloat(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    });
    // DOUBLE
    verifyForwardIndex(FieldSpec.DataType.DOUBLE, 10, (fwd) -> {
      assertEquals(fwd.getDouble(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    });
    // STRING
    verifyForwardIndex(FieldSpec.DataType.STRING, 10, (fwd) -> {
      assertEquals(fwd.getString(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertEquals(fwd.getStoredType(), FieldSpec.DataType.STRING);
    });
    // BYTES
    verifyForwardIndex(FieldSpec.DataType.BYTES, 10, (fwd) -> {
      assertEquals(fwd.getBytes(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    });
    // BIG_DECIMAL
    verifyForwardIndex(FieldSpec.DataType.BIG_DECIMAL, 10, (fwd) -> {
      assertEquals(fwd.getBigDecimal(0, null), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    });
  }

  @Test
  public void testForwardIndexIsSingleValueNotDictEncoded() {
    OpenStructNullDataSource ds = new OpenStructNullDataSource(
        new DimensionFieldSpec("k", FieldSpec.DataType.STRING, true), 5);
    ForwardIndexReader<?> fwd = ds.getForwardIndex();
    assertTrue(fwd.isSingleValue());
    assertFalse(fwd.isDictionaryEncoded());
  }

  @Test
  public void testNoDictionaryOrSecondaryIndexes() {
    OpenStructNullDataSource ds = new OpenStructNullDataSource(
        new DimensionFieldSpec("k", FieldSpec.DataType.STRING, true), 5);
    assertNull(ds.getDictionary());
    assertNull(ds.getInvertedIndex());
    assertNull(ds.getRangeIndex());
    assertNull(ds.getJsonIndex());
    assertNull(ds.getBloomFilter());
    assertNull(ds.getTextIndex());
    assertNull(ds.getVectorIndex());
  }

  @FunctionalInterface
  private interface ForwardIndexAssertion {
    void check(ForwardIndexReader<?> fwd);
  }

  private void verifyForwardIndex(FieldSpec.DataType type, int numDocs, ForwardIndexAssertion assertion) {
    OpenStructNullDataSource ds = new OpenStructNullDataSource(
        new DimensionFieldSpec("k", type, true), numDocs);
    assertion.check(ds.getForwardIndex());
  }
}
