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
package org.apache.pinot.core.operator.query;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.lang.reflect.Method;
import java.util.Set;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/**
 * Unit tests for the dictionary-based distinct value extraction in {@link NonScanBasedAggregationOperator},
 * specifically the LONG sortedness-detection loop: immutable dictionaries are value-sorted (fast path), while mutable
 * (realtime consuming segment) dictionaries are insertion-ordered and may contain any order — the detection must
 * classify them correctly, or DISTINCT results would silently over-count.
 */
public class NonScanBasedAggregationOperatorTest {

  private static Set invokeGetDistinctValueSet(Dictionary dictionary)
      throws Exception {
    Method method = NonScanBasedAggregationOperator.class.getDeclaredMethod("getDistinctValueSet", Dictionary.class);
    method.setAccessible(true);
    return (Set) method.invoke(null, dictionary);
  }

  private static Dictionary mockLongDictionary(long... values) {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.length()).thenReturn(values.length);
    for (int i = 0; i < values.length; i++) {
      when(dictionary.getLongValue(i)).thenReturn(values[i]);
    }
    return dictionary;
  }

  @Test
  public void testLongDistinctValuesFromSortedDictionary()
      throws Exception {
    // Immutable dictionary: value-sorted, duplicate-free
    Set set = invokeGetDistinctValueSet(mockLongDictionary(-5L, 0L, 3L, 42L, 1_000_000L));
    assertEquals(set.size(), 5);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{-5L, 0L, 3L, 42L, 1_000_000L}));
  }

  @Test
  public void testLongDistinctValuesFromUnsortedDictionary()
      throws Exception {
    // Mutable (realtime) dictionary: insertion-ordered, arbitrary order — the detection loop must fall back to
    // sort + dedupe rather than trusting sortedness
    Set set = invokeGetDistinctValueSet(mockLongDictionary(42L, -5L, 1_000_000L, 0L, 3L, 42L));
    assertEquals(set.size(), 5);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{-5L, 0L, 3L, 42L, 1_000_000L}));
  }

  @Test
  public void testLongDistinctValuesWithAdjacentDuplicates()
      throws Exception {
    // Ascending but with equal neighbors: must be classified as not sorted-distinct and deduped
    Set set = invokeGetDistinctValueSet(mockLongDictionary(1L, 1L, 2L, 3L, 3L));
    assertEquals(set.size(), 3);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{1L, 2L, 3L}));
  }

  @Test
  public void testLongDistinctValuesEmptyAndSingle()
      throws Exception {
    assertEquals(invokeGetDistinctValueSet(mockLongDictionary()).size(), 0);
    assertEquals(invokeGetDistinctValueSet(mockLongDictionary(7L)).size(), 1);
  }
}
