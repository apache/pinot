package com.linkedin.thirdeye;

import com.codahale.metrics.health.HealthCheck;

public class ThirdEyeHealthCheck extends HealthCheck
{
  @Override
  protected Result check() throws Exception
  {
    return Result.healthy();
  }
}
