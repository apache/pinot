package org.apache.pinot.common.metrics.prometheus.yammer;

import org.apache.pinot.common.metrics.prometheus.BrokerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;


public class YammerBrokerPrometheusMetricsTest extends BrokerPrometheusMetricsTest {

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
