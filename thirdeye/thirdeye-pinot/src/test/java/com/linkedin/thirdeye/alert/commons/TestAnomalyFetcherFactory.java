package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.fetcher.AnomalyFetcher;
import com.linkedin.thirdeye.alert.fetcher.ContinuumAnomalyFetcher;
import com.linkedin.thirdeye.alert.fetcher.UnnotifiedAnomalyFetcher;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestAnomalyFetcherFactory {

  @Test
  public void testCreateAlertFetcher() throws Exception{
    AnomalyFetcher anomalyFetcher = AnomalyFetcherFactory.fromClassName("ContinuumAnomalyFetcher");
    Assert.assertNotNull(anomalyFetcher);
    Assert.assertTrue(anomalyFetcher instanceof ContinuumAnomalyFetcher);

    anomalyFetcher = AnomalyFetcherFactory.fromClassName("UnnotifiedAnomalyFetcher");
    Assert.assertNotNull(anomalyFetcher);
    Assert.assertTrue(anomalyFetcher instanceof UnnotifiedAnomalyFetcher);
  }
}
