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
    int comparisonIndex = 0;
    Comparable[] newComparables = new Comparable[3];
    Comparable[] persistedComparables = new Comparable[3];
    ComparisonColumns alreadyPersisted = new ComparisonColumns(persistedComparables, comparisonIndex);
    ComparisonColumns toBeIngested = new ComparisonColumns(newComparables, comparisonIndex);

    // reject same col with smaller value
    newComparables[comparisonIndex] = 1;
    persistedComparables[comparisonIndex] = 2;
    int comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, -1);

    // persist same col with equal value
    nullFill(newComparables, persistedComparables);
    newComparables[comparisonIndex] = 2;
    persistedComparables[comparisonIndex] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 0);

    // persist same col with larger value
    nullFill(newComparables, persistedComparables);
    newComparables[comparisonIndex] = 2;
    persistedComparables[comparisonIndex] = 1;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // persist doc with col which was previously null, even though its value is smaller than the previous non-null col
    nullFill(newComparables, persistedComparables);
    comparisonIndex = newComparables.length - 1;
    toBeIngested = new ComparisonColumns(newComparables, comparisonIndex);
    newComparables[comparisonIndex] = 1;
    persistedComparables[0] = 2;
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // persist new doc where existing doc has multiple non-null comparison values
    nullFill(newComparables, persistedComparables);
    comparisonIndex = 1;
    toBeIngested = new ComparisonColumns(newComparables, comparisonIndex);
    newComparables[comparisonIndex] = 2;
    Arrays.fill(persistedComparables, 1);
    comparisonResult = toBeIngested.compareTo(alreadyPersisted);
    Assert.assertEquals(comparisonResult, 1);

    // reject new doc where existing doc has multiple non-null comparison values
    nullFill(newComparables, persistedComparables);
    newComparables[comparisonIndex] = 0;
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
