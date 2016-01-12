package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import com.linkedin.thirdeye.client.ThirdEyeClient;

public interface ThirdEyeClientFactory {
  public ThirdEyeClient getClient(Properties props);
}
