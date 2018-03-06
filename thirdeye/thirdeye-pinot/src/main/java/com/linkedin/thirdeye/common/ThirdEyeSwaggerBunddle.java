package com.linkedin.thirdeye.common;

import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;


public class ThirdEyeSwaggerBunddle extends SwaggerBundle<ThirdEyeConfiguration> {
  @Override
  protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(
      ThirdEyeConfiguration thirdEyeDashboardApplication) {
    SwaggerBundleConfiguration swaggerBundleConfiguration = thirdEyeDashboardApplication.swaggerBundleConfiguration;
    swaggerBundleConfiguration.setTitle("ThirdEye");
    swaggerBundleConfiguration.setDescription("ThirdEye REST endpoints");
    return swaggerBundleConfiguration;
  }
}
