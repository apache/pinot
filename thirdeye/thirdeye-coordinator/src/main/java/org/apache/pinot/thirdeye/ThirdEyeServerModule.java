package org.apache.pinot.thirdeye;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

public class ThirdEyeServerModule extends AbstractModule {

  public ThirdEyeServerModule(final ThirdEyeServerConfiguration configuration,
      final MetricRegistry metrics) {
  }

  @Override
  protected void configure() {

  }
}
