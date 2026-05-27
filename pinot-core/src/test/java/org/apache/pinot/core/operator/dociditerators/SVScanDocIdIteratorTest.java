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
package org.apache.pinot.core.operator.dociditerators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.BaseDictionaryBasedPredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SVScanDocIdIteratorTest {

  /// RAW forward index + separate dictionary + dictionary-based predicate evaluator
  /// (the layout used by external/iceberg-backed tables with IFST/FST/inverted-needing-dict).
  /// Without the DictLookupMatcher path, the scan would select `StringMatcher` and call
  /// `applySV(String)` on the dict-based evaluator, throwing `UnsupportedOperationException`.
  @Test
  public void rawForwardWithDictAndDictBasedEvaluatorRoutesThroughDictLookup() {
    String[] values = {"apple", "banana", "cherry", "apple", "date", "banana"};
    Map<String, Integer> dictEntries = new HashMap<>();
    dictEntries.put("apple", 0);
    dictEntries.put("banana", 1);
    dictEntries.put("cherry", 2);
    dictEntries.put("date", 3);

    ForwardIndexReader reader = mockRawStringReader(values);
    Dictionary dictionary = mockStringDictionary(dictEntries);
    // Matches dict ids {0, 2} → values "apple" and "cherry".
    BaseDictionaryBasedPredicateEvaluator evaluator = new TestDictPredicateEvaluator(dictionary, 0, 2);

    SVScanDocIdIterator iterator = new SVScanDocIdIterator(evaluator, reader, values.length, dictionary);

    assertEquals(iterator.next(), 0);   // "apple"
    assertEquals(iterator.next(), 2);   // "cherry"
    assertEquals(iterator.next(), 3);   // "apple"
    assertEquals(iterator.next(), Constants.EOF);
  }

  /// A raw value absent from the dictionary (`dict.indexOf(value)` returns -1) must be treated as a no-match
  /// without invoking `applySV` on the evaluator.
  @Test
  public void unknownValueDoesNotMatch() {
    String[] values = {"apple", "ghost", "cherry"};
    Map<String, Integer> dictEntries = new HashMap<>();
    dictEntries.put("apple", 0);
    dictEntries.put("cherry", 2);
    // "ghost" is not present → dict.indexOf("ghost") == -1.

    ForwardIndexReader reader = mockRawStringReader(values);
    Dictionary dictionary = mockStringDictionary(dictEntries);
    // Matches every known dict id; the only way "ghost" can match is if -1 leaks into applySV.
    BaseDictionaryBasedPredicateEvaluator evaluator = new TestDictPredicateEvaluator(dictionary, 0, 2);

    SVScanDocIdIterator iterator = new SVScanDocIdIterator(evaluator, reader, values.length, dictionary);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ForwardIndexReader mockRawStringReader(String[] values) {
    ForwardIndexReader reader = Mockito.mock(ForwardIndexReader.class);
    Mockito.when(reader.isDictionaryEncoded()).thenReturn(false);
    Mockito.when(reader.getStoredType()).thenReturn(DataType.STRING);
    Mockito.when(reader.createContext()).thenReturn(null);
    Mockito.when(reader.getString(Mockito.anyInt(), Mockito.any())).thenAnswer(invocation -> {
      int docId = invocation.getArgument(0);
      return values[docId];
    });
    return reader;
  }

  private static Dictionary mockStringDictionary(Map<String, Integer> entries) {
    Dictionary dictionary = Mockito.mock(Dictionary.class);
    Mockito.when(dictionary.length()).thenReturn(entries.size());
    Mockito.when(dictionary.indexOf(Mockito.anyString())).thenAnswer(invocation -> {
      String value = invocation.getArgument(0);
      Integer id = entries.get(value);
      return id != null ? id : -1;
    });
    return dictionary;
  }

  /// Minimal stand-in for IFST/FST/DictId-based regex evaluators: matches a fixed set of dict ids via
  /// `applySV(int dictId)`. Inherits the default `applySV(String)` that throws `UnsupportedOperationException`,
  /// so the test fails loudly if the scan picks the wrong matcher.
  private static class TestDictPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    private final Set<Integer> _matchingDictIds;

    TestDictPredicateEvaluator(Dictionary dictionary, int... matchingDictIds) {
      super(Mockito.mock(Predicate.class), dictionary);
      _matchingDictIds = new HashSet<>();
      for (int id : matchingDictIds) {
        _matchingDictIds.add(id);
      }
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictIds.contains(dictId);
    }
  }
}
