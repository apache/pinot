package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import com.linkedin.thirdeye.client.CachedThirdEyeClient;
import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class DefaultThirdEyeClientFactory implements ThirdEyeClientFactory {
  public static final String HOST = "host";
  public static final String PORT = "port";

  private final CachedThirdEyeClientConfig config;

  /** Returns a factory for creating cached default clients with default configurations. */
  public DefaultThirdEyeClientFactory() {
    this(true);
  }

  /**
   * Returns a factory that returns cached default clients if <tt>useCache</tt> is true, or regular
   * default clients if false.
   */
  public DefaultThirdEyeClientFactory(boolean useCache) {
    this(null, useCache);
  }

  /** Returns a factory for creating cached default client according to the provided config. */
  public DefaultThirdEyeClientFactory(CachedThirdEyeClientConfig config) {
    this(config, true);
  }

  private DefaultThirdEyeClientFactory(CachedThirdEyeClientConfig config, boolean useCache) {
    if (useCache && config == null) {
      config = new CachedThirdEyeClientConfig();
    } else if (!useCache) {
      config = null;
    }
    this.config = config;
  }

  @Override
  public ThirdEyeClient getClient(Properties props) {
    String hostname = props.getProperty(HOST);
    int port = Integer.valueOf(props.getProperty(PORT));
    return getClient(hostname, port);
  }

  public ThirdEyeClient getClient(String hostname, int port) {
    ThirdEyeClient client = new DefaultThirdEyeClient(hostname, port);
    if (config != null) {
      client = new CachedThirdEyeClient(client, config);
    }
    return client;
  }

}
