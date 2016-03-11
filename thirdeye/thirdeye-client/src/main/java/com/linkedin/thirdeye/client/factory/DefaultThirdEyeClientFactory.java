package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class DefaultThirdEyeClientFactory extends BaseThirdEyeClientFactory {
  public static final String HOST = "host";
  public static final String PORT = "port";

  public DefaultThirdEyeClientFactory() {

  }

  public DefaultThirdEyeClientFactory(CachedThirdEyeClientConfig config) {
    this.configureCache(config);
  }

  @Override
  public ThirdEyeClient getRawClient(Properties props) {
    assertContainsKeys(props, HOST, PORT);
    String hostname = props.getProperty(HOST);
    int port = Integer.valueOf(props.getProperty(PORT));
    return new DefaultThirdEyeClient(hostname, port);
  }

  public ThirdEyeClient getClient(String hostname, int port) {
    Properties properties = new Properties();
    properties.put(HOST, hostname);
    properties.put(PORT, port);
    return this.getClient(properties);
  }
}
