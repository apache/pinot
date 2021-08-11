package org.apache.pinot.broker.routing.segmentselector;

import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SelectedSegmentsTest {
  @Test
  public void testHashComputation() {
    SelectedSegments s1 = new SelectedSegments(ImmutableSet.of("seg1", "seg2"), true);
    SelectedSegments s2 = new SelectedSegments(ImmutableSet.of("seg2", "seg1"), true);
    Assert.assertEquals(s1.getSegmentHash(), s2.getSegmentHash());
  }
}