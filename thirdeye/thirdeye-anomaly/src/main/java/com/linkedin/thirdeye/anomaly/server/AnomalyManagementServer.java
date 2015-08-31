package com.linkedin.thirdeye.anomaly.server;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyDetectionConfiguration;
import com.linkedin.thirdeye.anomaly.server.resources.FunctionTableResource;

/**
 *
 */
public class AnomalyManagementServer extends Application<AnomalyManagementServerConfiguration> {

  private final ThirdEyeAnomalyDetectionConfiguration anomalyDetectionConfig;

  public AnomalyManagementServer(ThirdEyeAnomalyDetectionConfiguration anomalyDetectionConfig) {
    this.anomalyDetectionConfig = anomalyDetectionConfig;
  }

  @Override
  public void initialize(Bootstrap<AnomalyManagementServerConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
  }

  @Override
  public void run(AnomalyManagementServerConfiguration config, Environment environment) throws Exception {
    environment.jersey().register(new FunctionTableResource(anomalyDetectionConfig));
  }

}
