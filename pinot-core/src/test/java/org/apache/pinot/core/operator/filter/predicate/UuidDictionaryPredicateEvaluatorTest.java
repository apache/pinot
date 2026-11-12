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
package org.apache.pinot.core.operator.filter.predicate;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Unit test for dictionary-based predicate evaluators over a logical `UUID` column.
///
/// UUID has stored type BYTES, so it is backed by a plain bytes dictionary whose `getValueType()` reports BYTES. The
/// predicate value, however, arrives as a canonical UUID string rather than a hex string, so the UUID branches in the
/// evaluator factories look the value up as raw 16 bytes. These tests run against a real
/// [BytesOffHeapMutableDictionary] rather than a mock so that the lookup contract is exercised end to end.
public class UuidDictionaryPredicateEvaluatorTest {
  private static final ExpressionContext COLUMN_EXPRESSION = ExpressionContext.forIdentifier("column");
  private static final int NUM_VALUES = 32;

  private PinotDataBufferMemoryManager _memoryManager;
  private BytesOffHeapMutableDictionary _dictionary;
  private final List<String> _uuidStrings = new ArrayList<>(NUM_VALUES);

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(UuidDictionaryPredicateEvaluatorTest.class.getSimpleName());
    _dictionary = new BytesOffHeapMutableDictionary(NUM_VALUES, 0, _memoryManager, "uuidDictionary",
        UuidUtils.UUID_NUM_BYTES);
    for (int i = 0; i < NUM_VALUES; i++) {
      // Deterministic UUIDs so a failure is reproducible.
      UUID uuid = new UUID(0x0123456789abcdefL, i);
      _uuidStrings.add(uuid.toString());
      _dictionary.index(UuidUtils.toBytes(uuid));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _dictionary.close();
    _memoryManager.close();
  }

  /// The UUID fast path skips the hex round-trip that [PredicateUtils#getStoredValue] performs and looks the raw
  /// bytes up directly. Both must resolve to the same dictionary id, otherwise the fast path would silently change
  /// which rows match.
  @Test
  public void testStoredValueMatchesRawByteLookup() {
    for (String uuidString : _uuidStrings) {
      byte[] uuidBytes = UuidUtils.toBytes(uuidString);
      String storedValue = PredicateUtils.getStoredValue(uuidString, DataType.UUID);
      assertEquals(storedValue, BytesUtils.toHexString(uuidBytes));
      assertEquals(_dictionary.indexOf(storedValue), _dictionary.indexOf(new ByteArray(uuidBytes)));
    }
  }

  @Test
  public void testEqAndNeqEvaluators() {
    for (int i = 0; i < NUM_VALUES; i++) {
      String uuidString = _uuidStrings.get(i);
      int expectedDictId = _dictionary.indexOf(new ByteArray(UuidUtils.toBytes(uuidString)));
      assertTrue(expectedDictId >= 0);

      BaseDictionaryBasedPredicateEvaluator eqEvaluator = EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
          new EqPredicate(COLUMN_EXPRESSION, uuidString), _dictionary, DataType.UUID);
      assertEquals(eqEvaluator.getMatchingDictIds(), new int[]{expectedDictId});

      BaseDictionaryBasedPredicateEvaluator neqEvaluator =
          NotEqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
              new NotEqPredicate(COLUMN_EXPRESSION, uuidString), _dictionary, DataType.UUID);
      assertEquals(neqEvaluator.getNonMatchingDictIds(), new int[]{expectedDictId});
    }
  }

  @Test
  public void testEqEvaluatorIsCaseInsensitive() {
    String uuidString = _uuidStrings.get(0);
    int expectedDictId = _dictionary.indexOf(new ByteArray(UuidUtils.toBytes(uuidString)));

    BaseDictionaryBasedPredicateEvaluator eqEvaluator = EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
        new EqPredicate(COLUMN_EXPRESSION, uuidString.toUpperCase(Locale.ROOT)), _dictionary, DataType.UUID);
    assertEquals(eqEvaluator.getMatchingDictIds(), new int[]{expectedDictId});
  }

  @Test
  public void testEqAndNeqEvaluatorsOnAbsentValue() {
    // Same high bits as the indexed values but a low half outside the indexed range.
    String absentUuid = new UUID(0x0123456789abcdefL, NUM_VALUES).toString();
    assertTrue(_dictionary.indexOf(new ByteArray(UuidUtils.toBytes(absentUuid))) < 0);

    BaseDictionaryBasedPredicateEvaluator eqEvaluator = EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
        new EqPredicate(COLUMN_EXPRESSION, absentUuid), _dictionary, DataType.UUID);
    assertTrue(eqEvaluator.isAlwaysFalse());

    BaseDictionaryBasedPredicateEvaluator neqEvaluator = NotEqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
        new NotEqPredicate(COLUMN_EXPRESSION, absentUuid), _dictionary, DataType.UUID);
    assertTrue(neqEvaluator.isAlwaysTrue());
  }

  @Test
  public void testGetDictIdSet() {
    List<String> values = List.of(_uuidStrings.get(1), _uuidStrings.get(5), _uuidStrings.get(9),
        // An absent UUID must simply not contribute a dict id rather than fail the lookup.
        new UUID(0x0123456789abcdefL, NUM_VALUES + 1).toString());
    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, values);

    IntSet dictIdSet = PredicateUtils.getDictIdSet(inPredicate, _dictionary, DataType.UUID, null);
    assertEquals(dictIdSet.size(), 3);
    for (int i = 0; i < 3; i++) {
      assertTrue(dictIdSet.contains(_dictionary.indexOf(new ByteArray(UuidUtils.toBytes(values.get(i))))));
    }
  }

  @Test
  public void testInAndNotInEvaluators() {
    List<String> values = List.of(_uuidStrings.get(2), _uuidStrings.get(7));
    int[] expectedDictIds = new int[values.size()];
    for (int i = 0; i < values.size(); i++) {
      expectedDictIds[i] = _dictionary.indexOf(new ByteArray(UuidUtils.toBytes(values.get(i))));
    }

    BaseDictionaryBasedPredicateEvaluator inEvaluator = InPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
        new InPredicate(COLUMN_EXPRESSION, values), _dictionary, DataType.UUID, null);
    int[] matchingDictIds = inEvaluator.getMatchingDictIds();
    Arrays.sort(matchingDictIds);
    Arrays.sort(expectedDictIds);
    assertEquals(matchingDictIds, expectedDictIds);

    BaseDictionaryBasedPredicateEvaluator notInEvaluator = NotInPredicateEvaluatorFactory.newDictionaryBasedEvaluator(
        new NotInPredicate(COLUMN_EXPRESSION, values), _dictionary, DataType.UUID, null);
    int[] nonMatchingDictIds = notInEvaluator.getNonMatchingDictIds();
    Arrays.sort(nonMatchingDictIds);
    assertEquals(nonMatchingDictIds, expectedDictIds);
  }
}
