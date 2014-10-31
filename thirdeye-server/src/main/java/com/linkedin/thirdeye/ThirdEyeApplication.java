package com.linkedin.thirdeye;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ThirdEyeApplication extends Application<ThirdEyeConfiguration>
{
  private static final String NAME = "thirdeye";

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeConfiguration> thirdEyeConfigurationBootstrap)
  {
    // Do nothing
  }

  @Override
  public void run(ThirdEyeConfiguration thirdEyeConfiguration, Environment environment) throws Exception
  {
    final ThirdEyeMetricResource resource = new ThirdEyeMetricResource(null);
    final ThirdEyeHealthCheck healthCheck = new ThirdEyeHealthCheck();

    environment.healthChecks().register(NAME, healthCheck);
    environment.jersey().register(resource);
  }
}
