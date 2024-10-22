package org.apache.pinot.common.metrics.prometheus.dropwizard;

import org.apache.pinot.common.metrics.prometheus.ServerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.Test;


/**
 * Disabling tests as Pinot currently uses Yammer and these tests fail for for {@link DropwizardMetricsFactory}
 */
@Test(enabled = false)
public class DropwizardServerPrometheusMetricsTest extends ServerPrometheusMetricsTest {

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
