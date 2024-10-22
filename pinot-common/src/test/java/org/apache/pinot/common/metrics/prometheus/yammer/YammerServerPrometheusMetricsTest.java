package org.apache.pinot.common.metrics.prometheus.yammer;

import org.apache.pinot.common.metrics.prometheus.ServerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;


public class YammerServerPrometheusMetricsTest extends ServerPrometheusMetricsTest {

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
