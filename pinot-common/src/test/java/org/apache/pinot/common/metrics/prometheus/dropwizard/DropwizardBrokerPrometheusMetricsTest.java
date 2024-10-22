package org.apache.pinot.common.metrics.prometheus.dropwizard;

import org.apache.pinot.common.metrics.prometheus.BrokerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.Test;


/**
 * Disabling tests as Pinot currently uses Yammer and these tests fail for for {@link DropwizardMetricsFactory}
 */
@Test(enabled = false)
public class DropwizardBrokerPrometheusMetricsTest extends BrokerPrometheusMetricsTest {
  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new DropwizardMetricsFactory();
  }
}
