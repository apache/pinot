package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.feed.AnomalyFeed;
import com.linkedin.thirdeye.alert.feed.UnionAnomalyFeed;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestAnomalyFeedFactory {
  @Test
  public void testCreateAlertFeed() throws Exception{
    AnomalyFeed anomalyFeed = AnomalyFeedFactory.fromClassName("UnionAnomalyFeed");
    Assert.assertNotNull(anomalyFeed);
    Assert.assertTrue(anomalyFeed instanceof UnionAnomalyFeed);
  }
}
