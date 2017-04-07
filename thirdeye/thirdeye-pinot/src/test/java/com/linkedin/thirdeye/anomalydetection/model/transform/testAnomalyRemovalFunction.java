package com.linkedin.thirdeye.anomalydetection.model.transform;

import org.testng.Assert;
import org.testng.annotations.Test;

public class testAnomalyRemovalFunction {
  // create mock anomaly history
  // build mock AnomalyDetectionContext
  // create AnomalyRemovalFunction
  // compare data with expect case
  @Test
  public void testGetOffsetTimestamp () {
    String timezone = "America/Los_Angeles";
    long currentTS = 1491325200000L;  // 20170404 10:00:00 PDT
    long offSetUnit = 1000000L;
    int offSetSize = 5;
    long expected = currentTS - offSetSize * offSetUnit;
    long actual = AnomalyRemovalFunction.getOffsetTimestamp(currentTS, offSetSize, offSetUnit, timezone);
    Assert.assertEquals(actual, expected);

    // test daylight saving (20170312 begin on 2:00 AM)
    offSetUnit = 86400000L;  // one day
    offSetSize = 40;
    expected = currentTS - offSetSize * offSetUnit + 3600000L;
    actual = AnomalyRemovalFunction.getOffsetTimestamp(currentTS, offSetSize, offSetUnit, timezone);
    Assert.assertEquals(actual, expected);

    // if timezone is null, no daylight saving adjustment
    actual = AnomalyRemovalFunction.getOffsetTimestamp(currentTS, offSetSize, offSetUnit, null);
    expected = currentTS - offSetSize * offSetUnit;
    Assert.assertEquals(actual, expected);

  }

}
