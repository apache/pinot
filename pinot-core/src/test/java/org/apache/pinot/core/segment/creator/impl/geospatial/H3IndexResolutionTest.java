package org.apache.pinot.core.segment.creator.impl.geospatial;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public class H3IndexResolutionTest {

  @Test
  public void testH3IndexResolution() {
    H3IndexResolution resolution = new H3IndexResolution(Lists.newArrayList(13, 5, 6));
    Assert.assertEquals(resolution.size(), 3);
    Assert.assertEquals(resolution.getLowestResolution(), 5);
    Assert.assertEquals(resolution.getResolutions(), Lists.newArrayList(5, 6, 13));
  }
}
