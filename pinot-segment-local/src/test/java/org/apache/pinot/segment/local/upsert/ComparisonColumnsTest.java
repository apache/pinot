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
package org.apache.pinot.segment.local.upsert;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComparisonColumnsTest {
  private void nullFill(Comparable[]... comparables) {
    for (Comparable[] comps : comparables) {
      Arrays.fill(comps, null);
    }
  }

  @Test
  public void testRealtimeComparison() {
    Comparable[] newComparables = new Comparable[3];
    Comparable[] persistedComparables = new Comparable[3];
    ComparisonColumns alreadyPersisted = new ComparisonColumns(persistedComparables, 0);
    ComparisonColumns toBeIngested = new ComparisonColumns(newComparables, 0);

    // reject same col with smaller value
    newComparables[0] = 1;
    persistedComparables[0] = 2;
    int comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);

    // persist same col with equal value
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 2;
    persistedComparables[0] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 0);

    // persist same col with larger value
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 2;
    persistedComparables[0] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);
    Assert.assertEquals(toBeIngested.getValues(), new Comparable[]{2, null, null});

    // persist doc with col which was previously null, even though its value is smaller than the previous non-null col
    nullFill(newComparables, persistedComparables);
    toBeIngested = new ComparisonColumns(newComparables, newComparables.length - 1);
    newComparables[newComparables.length - 1] = 1;
    persistedComparables[0] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);
    Assert.assertEquals(toBeIngested.getValues(), new Comparable[]{2, null, 1});

    // persist new doc where existing doc has multiple non-null comparison values
    nullFill(newComparables, persistedComparables);
    toBeIngested = new ComparisonColumns(newComparables, 1);
    newComparables[1] = 2;
    Arrays.fill(persistedComparables, 1);
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);
    Assert.assertEquals(toBeIngested.getValues(), new Comparable[]{1, 2, 1});

    // reject new doc where existing doc has multiple non-null comparison values
    nullFill(newComparables, persistedComparables);
    newComparables[1] = 0;
    Arrays.fill(persistedComparables, 1);
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);
  }

  @Test
  public void testSealedComparison() {
    // Remember to be cognizant of which scenarios are _actually_ possible in a sealed segment. The way in which docs
    // are compared during realtime ingestion dictates the possible scenarios of persisted rows. Ex. it is not
    // possible for 2 docs with the same primary key to have a mutually exclusive set of non-null values; if such a
    // scenario arose during realtime ingestion, the values would be merged such that the newly persisted doc would
    // have all non-null comparison values. We should avoid making tests pass for scenarios that are not intended to
    // be supported.
    Comparable[] newComparables = new Comparable[3];
    Comparable[] persistedComparables = new Comparable[3];
    ComparisonColumns alreadyPersisted =
        new ComparisonColumns(persistedComparables, ComparisonColumns.SEALED_SEGMENT_COMPARISON_INDEX);
    ComparisonColumns toBeIngested =
        new ComparisonColumns(newComparables, ComparisonColumns.SEALED_SEGMENT_COMPARISON_INDEX);

    // reject same col with smaller value
    newComparables[0] = 1;
    persistedComparables[0] = 2;
    int comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);

    // persist same col with equal value
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 2;
    persistedComparables[0] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 0);
    // Verify unchanged comparables in the case of SEALED comparison
    Assert.assertEquals(toBeIngested.getValues(), newComparables);

    // persist same col with larger value
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 2;
    persistedComparables[0] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // reject doc where existing doc has more than one, but not all, non-null comparison values, but _this_ doc has 2
    // null columns. The presence of null columns in one of the docs implies that it must have come before the doc
    // with non-null columns.
    nullFill(newComparables, persistedComparables);
    newComparables[1] = 1;
    persistedComparables[0] = 1;
    persistedComparables[2] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);

    // persist doc where existing doc has more than one, but not all, non-null comparison values, but _this_ doc has
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 1;
    newComparables[2] = 2;
    persistedComparables[0] = 1;
    persistedComparables[2] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // persist doc with non-null value where existing doc had null value in same column previously (but multiple
    // non-null in other columns)
    nullFill(newComparables, persistedComparables);
    newComparables[0] = 1;
    newComparables[1] = 1;
    newComparables[2] = 1;
    persistedComparables[0] = 1;
    persistedComparables[2] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // reject doc where existing doc has all non-null comparison values, but _this_ doc has 2 null values.
    // The presence of null columns in one of the docs implies that it must have come before the doc with non-null
    // columns.
    nullFill(newComparables, persistedComparables);
    newComparables[1] = 1;
    Arrays.fill(persistedComparables, 1);
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);

    // Persist doc where existing doc has all non-null comparison values, but _this_ doc has a larger value.
    nullFill(newComparables, persistedComparables);
    Arrays.fill(newComparables, 1);
    Arrays.fill(persistedComparables, 1);
    newComparables[1] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);
  }
}
