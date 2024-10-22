package org.apache.pinot.common.metrics.prometheus.yammer;

import org.apache.pinot.common.metrics.prometheus.MinionPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;


public class YammerMinionPrometheusMetricsTest extends MinionPrometheusMetricsTest {

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
