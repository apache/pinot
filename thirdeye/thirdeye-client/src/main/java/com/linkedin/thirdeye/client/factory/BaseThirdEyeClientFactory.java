package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.linkedin.thirdeye.client.CachedThirdEyeClient;
import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/** Provides automatic caching functionality based on provided configuration. */
public abstract class BaseThirdEyeClientFactory implements ThirdEyeClientFactory {

  private CachedThirdEyeClientConfig cacheConfig;

  @Override
  public final ThirdEyeClient getClient(Properties props) {
    ThirdEyeClient client = getRawClient(props);
    if (cacheConfig != null) {
      client = new CachedThirdEyeClient(client, cacheConfig);
    }
    return client;
  }

  @Override
  public ThirdEyeClientFactory configureCache(CachedThirdEyeClientConfig config) {
    this.cacheConfig = config;
    return this;
  }

  /** Returns an uncached version of the client implementation provided by this factory. */
  protected abstract ThirdEyeClient getRawClient(Properties props);

  CachedThirdEyeClientConfig getConfig() {
    return cacheConfig;
  }

  protected static void assertContainsKeys(Properties props, String... keys) {
    if (!propsContainsKeys(props, keys)) {
      throw new IllegalArgumentException(
          "Properties must contain keys " + StringUtils.join(keys, ",") + ": " + props);
    }
  }

  protected static boolean propsContainsKeys(Properties props, String... keys) {
    if (props == null) {
      return false;
    }
    for (String key : keys) {
      if (!props.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

}
