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
package org.apache.pinot.query.service.dispatch;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link QueryErrorComparators}.
 *
 * <p>Key invariants verified:
 * <ol>
 *   <li>Comparator antisymmetry: {@code compare(a, b)} and {@code compare(b, a)} have opposite signs (or both 0).</li>
 *   <li>Two {@link QueryErrorCode#QUERY_VALIDATION} entries compare as equal (returns 0 in both directions), so
 *       TimSort cannot throw {@link IllegalArgumentException}.</li>
 *   <li>{@link QueryErrorCode#QUERY_VALIDATION} ranks higher than any non-validation error.</li>
 * </ol>
 */
public class QueryErrorComparatorsTest {

  private static Map.Entry<QueryErrorCode, String> entry(QueryErrorCode code, String msg) {
    return new AbstractMap.SimpleImmutableEntry<>(code, msg);
  }

  // -----------------------------------------------------------------------
  // Task 2: QUERY_VALIDATION tie-break — antisymmetry contract
  // -----------------------------------------------------------------------

  @Test
  public void testTwoQueryValidationEntriesAreEqual() {
    Map.Entry<QueryErrorCode, String> a = entry(QueryErrorCode.QUERY_VALIDATION, "first");
    Map.Entry<QueryErrorCode, String> b = entry(QueryErrorCode.QUERY_VALIDATION, "second");

    int ab = QueryErrorComparators.compareErrors(a, b);
    int ba = QueryErrorComparators.compareErrors(b, a);

    Assert.assertEquals(ab, 0, "compare(a,b) for two QUERY_VALIDATION entries must return 0");
    Assert.assertEquals(ba, 0, "compare(b,a) for two QUERY_VALIDATION entries must return 0");
  }

  @Test
  public void testQueryValidationRanksHigherThanInternal() {
    Map.Entry<QueryErrorCode, String> validation = entry(QueryErrorCode.QUERY_VALIDATION, "validation");
    Map.Entry<QueryErrorCode, String> internal = entry(QueryErrorCode.INTERNAL, "internal");

    int validationVsInternal = QueryErrorComparators.compareErrors(validation, internal);
    int internalVsValidation = QueryErrorComparators.compareErrors(internal, validation);

    Assert.assertTrue(validationVsInternal > 0,
        "QUERY_VALIDATION must rank higher than INTERNAL: compare(validation, internal) must be > 0");
    Assert.assertTrue(internalVsValidation < 0,
        "INTERNAL must rank lower than QUERY_VALIDATION: compare(internal, validation) must be < 0");
  }

  @Test
  public void testAntisymmetryForMixedPairs() {
    Map.Entry<QueryErrorCode, String> validation = entry(QueryErrorCode.QUERY_VALIDATION, "v");
    Map.Entry<QueryErrorCode, String> internal = entry(QueryErrorCode.INTERNAL, "i");

    int ab = QueryErrorComparators.compareErrors(validation, internal);
    int ba = QueryErrorComparators.compareErrors(internal, validation);

    // antisymmetry: sign(compare(a,b)) == -sign(compare(b,a))
    Assert.assertTrue((ab > 0 && ba < 0) || (ab < 0 && ba > 0) || (ab == 0 && ba == 0),
        "Comparator must be antisymmetric: compare(a,b) and compare(b,a) must have opposite signs (or both 0)");
  }

  // -----------------------------------------------------------------------
  // Task 3: Sort-stability under TimSort (≥32 elements to exercise merge sort)
  // -----------------------------------------------------------------------

  /**
   * Builds a list of &ge;32 entries containing multiple QUERY_VALIDATION entries plus INTERNAL errors,
   * then sorts it.  TimSort uses merge sort for ≥32 elements and would throw
   * {@link IllegalArgumentException} if the comparator violated antisymmetry.
   *
   * <p>After sorting (ascending), all QUERY_VALIDATION entries must appear at the end because they are
   * the maximum; we use {@link Collections#max} which relies on the same comparator.
   */
  @Test
  public void testSortWithMultipleQueryValidationEntriesDoesNotThrow() {
    List<Map.Entry<QueryErrorCode, String>> entries = new ArrayList<>();

    // add many INTERNAL errors
    for (int i = 0; i < 20; i++) {
      entries.add(entry(QueryErrorCode.INTERNAL, "internal-" + i));
    }
    // add multiple QUERY_VALIDATION errors (the problematic case before the fix)
    for (int i = 0; i < 15; i++) {
      entries.add(entry(QueryErrorCode.QUERY_VALIDATION, "validation-" + i));
    }
    // total: 35 entries — forces TimSort's merge path

    // Must not throw IllegalArgumentException
    entries.sort(QueryErrorComparators::compareErrors);

    // (b) QUERY_VALIDATION must rank highest — max() via the comparator picks a QUERY_VALIDATION entry
    Map.Entry<QueryErrorCode, String> maxEntry =
        entries.stream().max(QueryErrorComparators::compareErrors).orElseThrow();
    Assert.assertEquals(maxEntry.getKey(), QueryErrorCode.QUERY_VALIDATION,
        "The entry with the highest priority must be a QUERY_VALIDATION error");

    // After ascending sort, the last element must also be QUERY_VALIDATION
    Map.Entry<QueryErrorCode, String> last = entries.get(entries.size() - 1);
    Assert.assertEquals(last.getKey(), QueryErrorCode.QUERY_VALIDATION,
        "After ascending sort, the last (max) element must be QUERY_VALIDATION");
  }
}
