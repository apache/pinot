package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.DefaultThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class DefaultThirdEyeClientFactory implements ThirdEyeClientFactory {
  public static final String HOST = "host";
  public static final String PORT = "port";

  private final DefaultThirdEyeClientConfig config;

  public DefaultThirdEyeClientFactory() {
    this(null);
  }

  public DefaultThirdEyeClientFactory(DefaultThirdEyeClientConfig config) {
    this.config = config;
  }

  @Override
  public ThirdEyeClient getClient(Properties props) {
    String hostname = props.getProperty(HOST);
    int port = Integer.valueOf(props.getProperty(PORT));
    return build(hostname, port);
  }

  public ThirdEyeClient build(String hostname, int port) {
    if (config != null) {
      return new DefaultThirdEyeClient(hostname, port, config);
    } else {
      return new DefaultThirdEyeClient(hostname, port);
    }
  }

}
