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
package org.apache.pinot.segment.local.segment.index.datasource;

import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Covers [NullDataSource], the data source for a column holding no value in any document. Asserts it is built
/// like a default column — a single-entry dictionary over the field's default null value, constant sorted or
/// multi-value readers, and metadata with cardinality one — and that every document is additionally marked null.
public class NullDataSourceTest {
  private static final int NUM_DOCS = 100;

  @Test
  public void testBuiltLikeDefaultColumn() {
    NullDataSource dataSource = new NullDataSource(new DimensionFieldSpec("k", DataType.LONG, true), NUM_DOCS);

    DataSourceMetadata metadata = dataSource.getDataSourceMetadata();
    assertEquals(metadata.getDataType(), DataType.LONG);
    assertTrue(metadata.isSingleValue());
    assertTrue(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), NUM_DOCS);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), -1);
    assertEquals(metadata.getCardinality(), 1);
    assertEquals(metadata.getMinValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(metadata.getMaxValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);

    Dictionary dictionary = dataSource.getDictionary();
    assertNotNull(dictionary);
    assertEquals(dictionary.length(), 1);
    assertEquals(dictionary.getLongValue(0), (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);

    ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
    assertTrue(forwardIndex.isDictionaryEncoded());
    assertTrue(forwardIndex.isSingleValue());
    assertEquals(forwardIndex.getDictId(NUM_DOCS - 1, null), 0);

    SortedIndexReader<?> sortedIndex = (SortedIndexReader<?>) dataSource.getInvertedIndex();
    assertEquals(sortedIndex.getDocIds(0).getLeft(), 0);
    assertEquals(sortedIndex.getDocIds(0).getRight(), NUM_DOCS - 1);

    assertNull(dataSource.getRangeIndex());
    assertNull(dataSource.getJsonIndex());
    assertNull(dataSource.getBloomFilter());
    assertNull(dataSource.getTextIndex());
    assertNull(dataSource.getVectorIndex());
  }

  @Test
  public void testEveryDocumentIsNull() {
    NullDataSource dataSource = new NullDataSource(new DimensionFieldSpec("k", DataType.INT, true), 50);
    NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
    assertNotNull(nullValueVector);
    for (int i = 0; i < 50; i++) {
      assertTrue(nullValueVector.isNull(i));
    }
    assertEquals(nullValueVector.getNullBitmap().getCardinality(), 50);

    dataSource = new NullDataSource(new DimensionFieldSpec("k", DataType.INT, true), 0);
    assertEquals(dataSource.getNullValueVector().getNullBitmap().getCardinality(), 0);
  }

  @Test
  public void testDictionaryHoldsDefaultNullValue() {
    assertEquals(dictionaryValue(DataType.INT), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(dictionaryValue(DataType.LONG), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(dictionaryValue(DataType.FLOAT), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    assertEquals(dictionaryValue(DataType.DOUBLE), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    assertEquals(dictionaryValue(DataType.BIG_DECIMAL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    assertEquals(dictionaryValue(DataType.BOOLEAN), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN);
    assertEquals(dictionaryValue(DataType.TIMESTAMP), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(dictionaryValue(DataType.STRING), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(dictionaryValue(DataType.JSON), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
    assertEquals(dictionaryValue(DataType.BYTES), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
  }

  @Test
  public void testCustomDefaultNullValue() {
    NullDataSource dataSource = new NullDataSource(new DimensionFieldSpec("k", DataType.STRING, true, "N/A"), NUM_DOCS);
    assertEquals(dataSource.getDictionary().get(0), "N/A");
    assertEquals(dataSource.getDataSourceMetadata().getMinValue(), "N/A");
    assertEquals(dataSource.getDataSourceMetadata().getMaxValue(), "N/A");
  }

  @Test
  public void testMultiValue() {
    NullDataSource dataSource = new NullDataSource(new DimensionFieldSpec("k", DataType.INT, false), NUM_DOCS);

    DataSourceMetadata metadata = dataSource.getDataSourceMetadata();
    assertFalse(metadata.isSingleValue());
    assertFalse(metadata.isSorted());
    assertEquals(metadata.getNumValues(), NUM_DOCS);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), 1);

    ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
    assertTrue(forwardIndex.isDictionaryEncoded());
    assertFalse(forwardIndex.isSingleValue());
    assertEquals(forwardIndex.getNumValuesMV(0, null), 1);
    assertEquals(forwardIndex.getDictIdMV(0, null), new int[]{0});
    ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) dataSource.getInvertedIndex().getDocIds(0);
    assertEquals(docIds.getCardinality(), NUM_DOCS);
  }

  private static Object dictionaryValue(DataType dataType) {
    return new NullDataSource(new DimensionFieldSpec("k", dataType, true), NUM_DOCS).getDictionary().get(0);
  }
}
