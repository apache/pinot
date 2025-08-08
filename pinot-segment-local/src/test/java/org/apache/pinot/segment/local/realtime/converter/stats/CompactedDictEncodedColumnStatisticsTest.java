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


public class CompactedDictEncodedColumnStatisticsTest {

  @Test
  public void testSingleValueColumn() {
    // Test single-value dictionary-encoded column
    DataSource dataSource = createMockDataSource("testColumn", true); // single-value
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(0); // Duplicate value
    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    Dictionary dictionary = createMockDictionary(new String[]{"value1", "value2"});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    // Valid doc IDs: 0, 1, 2
    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 1, 2);

    CompactedDictEncodedColumnStatistics stats =
        new CompactedDictEncodedColumnStatistics(dataSource, null, validDocIds);

    // Should find 2 unique dictionary IDs (0 and 1)
    Assert.assertEquals(stats.getCardinality(), 2);

    // Verify unique values array contains both values
    String[] uniqueValues = (String[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 2);
    Assert.assertTrue(uniqueValues[0].equals("value1") || uniqueValues[0].equals("value2"));
    Assert.assertTrue(uniqueValues[1].equals("value1") || uniqueValues[1].equals("value2"));
    Assert.assertNotEquals(uniqueValues[0], uniqueValues[1]);
  }

  @Test
  public void testMultiValueColumn() {
    // Test multi-value dictionary-encoded column (this tests the fix)
    DataSource dataSource = createMockDataSource("tagsColumn", false); // multi-value
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);

    // Mock multi-value responses
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1}); // ["tag1", "tag2"]
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{1, 2}); // ["tag2", "tag3"]
    when(forwardIndex.getDictIdMV(2)).thenReturn(new int[]{0}); // ["tag1"]

    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    Dictionary dictionary = createMockDictionary(new String[]{"tag1", "tag2", "tag3"});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    // Valid doc IDs: 0, 1, 2
    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 1, 2);

    CompactedDictEncodedColumnStatistics stats =
        new CompactedDictEncodedColumnStatistics(dataSource, null, validDocIds);

    // Should find 3 unique dictionary IDs (0, 1, 2) across all multi-value entries
    Assert.assertEquals(stats.getCardinality(), 3);

    // Verify unique values array contains all three values
    String[] uniqueValues = (String[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 3);
    // Values should be sorted: tag1, tag2, tag3
    Assert.assertEquals(uniqueValues[0], "tag1");
    Assert.assertEquals(uniqueValues[1], "tag2");
    Assert.assertEquals(uniqueValues[2], "tag3");
  }

  @Test
  public void testMultiValueColumnWithPartialValidDocs() {
    // Test multi-value column where only some documents are valid (commit-time compaction scenario)
    DataSource dataSource = createMockDataSource("tagsColumn", false); // multi-value
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);

    // Mock multi-value responses for docs 0-3, but only 0 and 2 are valid
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1}); // ["tag1", "tag2"] - VALID
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2, 3}); // ["tag3", "tag4"] - INVALID (not in validDocIds)
    when(forwardIndex.getDictIdMV(2)).thenReturn(new int[]{1, 4}); // ["tag2", "tag5"] - VALID
    when(forwardIndex.getDictIdMV(3)).thenReturn(new int[]{3, 4}); // ["tag4", "tag5"] - INVALID (not in validDocIds)

    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    Dictionary dictionary = createMockDictionary(new String[]{"tag1", "tag2", "tag3", "tag4", "tag5"});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    // Only docs 0 and 2 are valid (simulating commit-time compaction filtering)
    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 2);

    CompactedDictEncodedColumnStatistics stats =
        new CompactedDictEncodedColumnStatistics(dataSource, null, validDocIds);

    // Should only find dictionary IDs from valid docs: {0, 1, 4} -> {"tag1", "tag2", "tag5"}
    Assert.assertEquals(stats.getCardinality(), 3);

    String[] uniqueValues = (String[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 3);
    Assert.assertEquals(uniqueValues[0], "tag1");
    Assert.assertEquals(uniqueValues[1], "tag2");
    Assert.assertEquals(uniqueValues[2], "tag5");

    // Should NOT contain tag3 or tag4 since those came from invalid docs
  }

  @Test
  public void testMixedColumnTypes() {
    // Test with Long data type (not just String)
    DataSource dataSource = createMockDataSource("regionIDColumn", true); // single-value Long
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(1);
    when(forwardIndex.getDictId(1)).thenReturn(2);
    when(forwardIndex.getDictId(2)).thenReturn(1); // Duplicate
    when(dataSource.getForwardIndex()).thenReturn(
        (org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) forwardIndex);

    Dictionary dictionary = createMockLongDictionary(new Long[]{100L, 200L, 300L});
    when(dataSource.getDictionary()).thenReturn(dictionary);

    ThreadSafeMutableRoaringBitmap validDocIds = createValidDocIds(0, 1, 2);

    CompactedDictEncodedColumnStatistics stats =
        new CompactedDictEncodedColumnStatistics(dataSource, null, validDocIds);

    Assert.assertEquals(stats.getCardinality(), 2); // IDs 1 and 2 used

    Long[] uniqueValues = (Long[]) stats.getUniqueValuesSet();
    Assert.assertEquals(uniqueValues.length, 2);
    Assert.assertEquals(uniqueValues[0], Long.valueOf(200L)); // Dict ID 1
    Assert.assertEquals(uniqueValues[1], Long.valueOf(300L)); // Dict ID 2
  }

  // Helper methods

  private DataSource createMockDataSource(String columnName, boolean isSingleValue) {
    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);

    FieldSpec.DataType dataType = isSingleValue ? FieldSpec.DataType.STRING : FieldSpec.DataType.STRING;
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(columnName, dataType, isSingleValue);

    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);

    return dataSource;
  }

  private Dictionary createMockDictionary(String[] values) {
    Dictionary dictionary = mock(Dictionary.class);

    for (int i = 0; i < values.length; i++) {
      when(dictionary.get(i)).thenReturn(values[i]);
    }

    when(dictionary.getSortedValues()).thenReturn(values);
    return dictionary;
  }

  private Dictionary createMockLongDictionary(Long[] values) {
    Dictionary dictionary = mock(Dictionary.class);

    for (int i = 0; i < values.length; i++) {
      when(dictionary.get(i)).thenReturn(values[i]);
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
