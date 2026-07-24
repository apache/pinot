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


/// Unit tests for the dictionary-based distinct value extraction in [NonScanBasedAggregationOperator] for LONG
/// columns: immutable dictionaries are value-sorted (`isSorted()` = true, wrapped as-is), while mutable (realtime
/// consuming segment) dictionaries are insertion-ordered (`isSorted()` = false) and must be sorted first — otherwise
/// DISTINCT results would be silently wrong.
public class NonScanBasedAggregationOperatorTest {

  private static Set invokeGetDistinctValueSet(Dictionary dictionary)
      throws Exception {
    Method method = NonScanBasedAggregationOperator.class.getDeclaredMethod("getDistinctValueSet", Dictionary.class);
    method.setAccessible(true);
    return (Set) method.invoke(null, dictionary);
  }

  private static Dictionary mockLongDictionary(boolean sorted, long... values) {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.length()).thenReturn(values.length);
    when(dictionary.isSorted()).thenReturn(sorted);
    for (int i = 0; i < values.length; i++) {
      when(dictionary.getLongValue(i)).thenReturn(values[i]);
    }
    return dictionary;
  }

  @Test
  public void testLongDistinctValuesFromSortedDictionary()
      throws Exception {
    // Immutable dictionary: value-sorted, duplicate-free
    Set set = invokeGetDistinctValueSet(mockLongDictionary(true, -5L, 0L, 3L, 42L, 1_000_000L));
    assertEquals(set.size(), 5);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{-5L, 0L, 3L, 42L, 1_000_000L}));
  }

  @Test
  public void testLongDistinctValuesFromUnsortedDictionary()
      throws Exception {
    // Mutable (realtime) dictionary: insertion-ordered (isSorted() = false), so the values must be sorted before
    // being wrapped as a sorted run
    Set set = invokeGetDistinctValueSet(mockLongDictionary(false, 42L, -5L, 1_000_000L, 0L, 3L));
    assertEquals(set.size(), 5);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{-5L, 0L, 3L, 42L, 1_000_000L}));
  }

  @Test
  public void testLongDistinctValuesFromAscendingDictionaryReportedUnsorted()
      throws Exception {
    // A mutable dictionary whose insertions happened to arrive ascending still reports isSorted() = false; the
    // redundant sort must not change the result
    Set set = invokeGetDistinctValueSet(mockLongDictionary(false, 1L, 2L, 3L));
    assertEquals(set.size(), 3);
    assertEquals(new LongOpenHashSet(set), new LongOpenHashSet(new long[]{1L, 2L, 3L}));
  }

  @Test
  public void testLongDistinctValuesEmptyAndSingle()
      throws Exception {
    assertEquals(invokeGetDistinctValueSet(mockLongDictionary(true)).size(), 0);
    assertEquals(invokeGetDistinctValueSet(mockLongDictionary(false, 7L)).size(), 1);
  }
}
