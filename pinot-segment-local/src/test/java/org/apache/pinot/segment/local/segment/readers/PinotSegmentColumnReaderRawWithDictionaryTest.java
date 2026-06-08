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
package org.apache.pinot.segment.local.segment.readers;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Unit tests for {@link PinotSegmentColumnReader#getDictId(int)} when the forward index is RAW
/// and a separate (shared) dictionary is materialized for the column. This is the encoding used by
/// star-tree dimension columns that opt into shared dictionaries — the FI stores raw values, and
/// the star-tree builder needs to resolve a dict-id per doc by looking the raw value up in the
/// dictionary.
public class PinotSegmentColumnReaderRawWithDictionaryTest {

  @DataProvider(name = "rawTypes")
  public Object[][] rawTypes() {
    return new Object[][]{
        {DataType.INT, 42, 7},
        {DataType.LONG, 9_000_000_000L, 11},
        {DataType.FLOAT, 3.14f, 3},
        {DataType.DOUBLE, 2.71828d, 5},
        {DataType.BIG_DECIMAL, new BigDecimal("123.456"), 17},
        {DataType.STRING, "hello-world", 23},
    };
  }

  @Test(dataProvider = "rawTypes")
  public void testGetDictIdResolvesFromDictionaryForRawForwardIndex(DataType type, Object value,
      int expectedDictId)
      throws Exception {
    ForwardIndexReader<?> fiReader = mock(ForwardIndexReader.class);
    Dictionary dictionary = mock(Dictionary.class);
    when(fiReader.isSingleValue()).thenReturn(true);
    when(fiReader.isDictionaryEncoded()).thenReturn(false);
    when(fiReader.getStoredType()).thenReturn(type);
    when(dictionary.getValueType()).thenReturn(type);

    switch (type) {
      case INT:
        when(fiReader.getInt(eq(0), any())).thenReturn((Integer) value);
        when(dictionary.indexOf((int) (Integer) value)).thenReturn(expectedDictId);
        break;
      case LONG:
        when(fiReader.getLong(eq(0), any())).thenReturn((Long) value);
        when(dictionary.indexOf((long) (Long) value)).thenReturn(expectedDictId);
        break;
      case FLOAT:
        when(fiReader.getFloat(eq(0), any())).thenReturn((Float) value);
        when(dictionary.indexOf((float) (Float) value)).thenReturn(expectedDictId);
        break;
      case DOUBLE:
        when(fiReader.getDouble(eq(0), any())).thenReturn((Double) value);
        when(dictionary.indexOf((double) (Double) value)).thenReturn(expectedDictId);
        break;
      case BIG_DECIMAL:
        when(fiReader.getBigDecimal(eq(0), any())).thenReturn((BigDecimal) value);
        when(dictionary.indexOf((BigDecimal) value)).thenReturn(expectedDictId);
        break;
      case STRING:
        when(fiReader.getString(eq(0), any())).thenReturn((String) value);
        when(dictionary.indexOf((String) value)).thenReturn(expectedDictId);
        break;
      default:
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    PinotSegmentColumnReader reader = new PinotSegmentColumnReader(fiReader, dictionary, null, 0);
    assertEquals(reader.getDictId(0), expectedDictId);
  }

  @Test
  public void testGetDictIdResolvesFromDictionaryForRawBytesForwardIndex()
      throws Exception {
    ForwardIndexReader<?> fiReader = mock(ForwardIndexReader.class);
    Dictionary dictionary = mock(Dictionary.class);
    byte[] raw = new byte[]{1, 2, 3, 4};
    int expectedDictId = 13;

    when(fiReader.isSingleValue()).thenReturn(true);
    when(fiReader.isDictionaryEncoded()).thenReturn(false);
    when(fiReader.getStoredType()).thenReturn(DataType.BYTES);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(fiReader.getBytes(eq(0), any())).thenReturn(raw);
    when(dictionary.indexOf(new ByteArray(raw))).thenReturn(expectedDictId);

    PinotSegmentColumnReader reader = new PinotSegmentColumnReader(fiReader, dictionary, null, 0);
    assertEquals(reader.getDictId(0), expectedDictId);
  }

  @Test
  public void testGetDictIdDelegatesToForwardIndexWhenDictionaryEncoded() {
    ForwardIndexReader<?> fiReader = mock(ForwardIndexReader.class);
    Dictionary dictionary = mock(Dictionary.class);
    when(fiReader.isSingleValue()).thenReturn(true);
    when(fiReader.isDictionaryEncoded()).thenReturn(true);
    when(fiReader.getStoredType()).thenReturn(DataType.INT);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(fiReader.getDictId(eq(3), any())).thenReturn(99);

    PinotSegmentColumnReader reader = new PinotSegmentColumnReader(fiReader, dictionary, null, 0);
    assertEquals(reader.getDictId(3), 99);
    verify(fiReader).getDictId(eq(3), any());
    // No raw-value lookup should be attempted in the dictionary-encoded path.
    verify(dictionary, org.mockito.Mockito.never()).indexOf(anyInt());
  }

  @Test
  public void testGetDictIdThrowsWhenRawAndNoDictionaryMaterialized() {
    ForwardIndexReader<?> fiReader = mock(ForwardIndexReader.class);
    when(fiReader.isSingleValue()).thenReturn(true);
    when(fiReader.isDictionaryEncoded()).thenReturn(false);
    when(fiReader.getStoredType()).thenReturn(DataType.INT);

    PinotSegmentColumnReader reader = new PinotSegmentColumnReader(fiReader, null, null, 0);
    assertThrows(UnsupportedOperationException.class, () -> reader.getDictId(0));
  }
}
