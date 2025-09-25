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
package org.apache.pinot.segment.local.realtime.converter.stats;

import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class CompactedRawIndexDictColumnStatisticsTest {

  @Test
  public void testRawIndexWithDictionary() {
    // Test column with dictionary but raw (non-dictionary-encoded) forward index
    DataSource dataSource = createMockDataSource("rawColumn", true); // single-value
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false); // Raw index
    when(forwardIndex.getStoredType()).thenReturn(FieldSpec.DataType.STRING);

    // Mock raw value reads
    when(forwardIndex.getString(0)).thenReturn("value1");
    when(forwardIndex.getString(1)).thenReturn("value2");
    when(forwardIndex.getString(2)).thenReturn("value1"); // Duplicate

    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    // Dictionary exists but forward index stores raw values
    Dictionary dictionary = createMockDictionary(new String[]{"value1", "value2", "value3"});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    // Valid doc IDs: 0, 1, 2
    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 1, 2);

    CompactedRawIndexDictColumnStatistics stats =
        new CompactedRawIndexDictColumnStatistics(dataSource, null, validDocIds);

    // Should find 2 unique values from the raw reads
    Assert.assertEquals(stats.getCardinality(), 2);

    // Verify unique values array contains both values
    String[] uniqueValues = (String[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 2);
    Assert.assertEquals(uniqueValues[0], "value1");
    Assert.assertEquals(uniqueValues[1], "value2");
  }

  @Test
  public void testRawIndexWithPartialValidDocs() {
    // Test raw index where only some documents are valid (commit-time compaction scenario)
    DataSource dataSource = createMockDataSource("rawColumn", true); // single-value
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false); // Raw index
    when(forwardIndex.getStoredType()).thenReturn(FieldSpec.DataType.STRING);

    // Mock raw value reads for docs 0-3, but only 0 and 2 are valid
    when(forwardIndex.getString(0)).thenReturn("validValue1");   // VALID
    when(forwardIndex.getString(1)).thenReturn("invalidValue");  // INVALID (not in validDocIds)
    when(forwardIndex.getString(2)).thenReturn("validValue2");   // VALID
    when(forwardIndex.getString(3)).thenReturn("invalidValue2"); // INVALID (not in validDocIds)

    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    Dictionary dictionary =
        createMockDictionary(new String[]{"validValue1", "validValue2", "invalidValue", "invalidValue2"});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    // Only docs 0 and 2 are valid (simulating commit-time compaction filtering)
    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 2);

    CompactedRawIndexDictColumnStatistics stats =
        new CompactedRawIndexDictColumnStatistics(dataSource, null, validDocIds);

    // Should only find values from valid docs
    Assert.assertEquals(stats.getCardinality(), 2);

    String[] uniqueValues = (String[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 2);
    Assert.assertEquals(uniqueValues[0], "validValue1");
    Assert.assertEquals(uniqueValues[1], "validValue2");

    // Should NOT contain invalidValue or invalidValue2
  }

  // Helper methods

  private DataSource createMockDataSource(String columnName, boolean isSingleValue) {
    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);

    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(columnName, FieldSpec.DataType.STRING, isSingleValue);

    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);

    return dataSource;
  }

  private Dictionary createMockDictionary(String[] values) {
    Dictionary dictionary = mock(Dictionary.class);

    // Mock dictionary lookups by value
    for (int i = 0; i < values.length; i++) {
      when(dictionary.get(i)).thenReturn(values[i]);
      when(dictionary.indexOf(values[i])).thenReturn(i);
    }

    when(dictionary.getSortedValues()).thenReturn(values);
    return dictionary;
  }

  private ThreadSafeMutableRoaringBitmap createValidDocIds(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId : docIds) {
      bitmap.add(docId);
    }

    ThreadSafeMutableRoaringBitmap safeBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    when(safeBitmap.getMutableRoaringBitmap()).thenReturn(bitmap);

    return safeBitmap;
  }
}
