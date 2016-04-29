package com.linkedin.thirdeye.hadoop.push;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class SegmentPushControllerAPIsTest {

  private String[] controllerHosts = null;
  private String controllerPort = "0";
  private String testTable1 = "testTable";
  private String testTable2 = "test_table";
  SegmentPushControllerAPIs segmentPushControllerAPIs;

  @BeforeClass
  public void setup() {
    segmentPushControllerAPIs = new SegmentPushControllerAPIs(controllerHosts, controllerPort);
  }

  @Test
  public void testOverlapPattern() throws Exception {

    String segmentName = testTable1 + "_DAILY_2016-04-28-000000_2016-04-29-000000";
    String overlapPattern = segmentPushControllerAPIs.getOverlapPattern(segmentName, testTable1);
    Assert.assertEquals(overlapPattern, testTable1 + "_HOURLY_2016-04-28", "Incorrect overlap pattern for segment " + segmentName);

    segmentName = testTable2 + "_DAILY_2016-04-28-000000_2016-04-29-000000";
    overlapPattern = segmentPushControllerAPIs.getOverlapPattern(segmentName, testTable2);
    Assert.assertEquals(overlapPattern, testTable2 + "_HOURLY_2016-04-28", "Incorrect overlap pattern for segment " + segmentName);
  }

  @Test
  public void testGetOverlappingSegments() throws Exception {
    List<String> allSegments = Lists.newArrayList(
        "test_HOURLY_2016-04-28-000000_2016-04-28-010000",
        "test_HOURLY_2016-04-28-230000_2016-04-29-000000",
        "test_DAILY_2016-04-28-000000_2016-04-29-000000");
    String pattern = "test_HOURLY_2016-04-28";
    List<String> overlappingSegments = segmentPushControllerAPIs.getOverlappingSegments(allSegments, pattern);
    allSegments.remove(2);
    Assert.assertEquals(overlappingSegments, allSegments, "Incorrect overlapping segments returned");
  }

}
