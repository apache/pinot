package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.fetcher.AnomalyFetcher;
import com.linkedin.thirdeye.alert.fetcher.ContinuumAnomalyFetcher;
import com.linkedin.thirdeye.alert.fetcher.UnnotifiedAnomalyFetcher;
import org.testng.annotations.Test;


public class TestAnomalyFetcherFactory {

  @Test
  public void testCreateAlertFetcher() throws Exception{
    AnomalyFetcher anomalyFetcher = AnomalyFetcherFactory.fromClassName("ContinuumAnomalyFetcher");
    assert anomalyFetcher != null;
    assert anomalyFetcher instanceof ContinuumAnomalyFetcher;

    anomalyFetcher = AnomalyFetcherFactory.fromClassName("UnnotifiedAnomalyFetcher");
    assert anomalyFetcher != null;
    assert anomalyFetcher instanceof UnnotifiedAnomalyFetcher;
  }
}
