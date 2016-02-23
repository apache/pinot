package com.linkedin.thirdeye.client.util;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Objects;
import com.linkedin.thirdeye.client.CachedThirdEyeClient;
import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.factory.ThirdEyeClientFactory;

/**
 * Configuration class - this file should be in a yaml format such as in test-clients.yml. </br>
 * <ul>
 * <li>factoryClassName: full qualified name of a {@link ThirdEyeClientFactory} implementation.
 * The implementation should provide a no-args constructor and implement the
 * {@link ThirdEyeClientFactory#configureCache(CachedThirdEyeClientConfig)} method.
 * <li>clientArgs: mapping for arguments used in constructing {@link ThirdEyeClient} with the
 * specified factory.
 * <li>cached: determines whether the client should be wrapped by {@link CachedThirdEyeClient}. By
 * default it is true and uses the default cache configuration.
 * <li>cacheConfig: provides custom cache configuration. This property should not be provided if
 * <tt>cached</tt> is set to false.
 * </ul>
 * @author jteoh
 */
public class ThirdEyeClientConfig {
  private String name;
  private String factoryClassName;
  private CachedThirdEyeClientConfig cacheConfig;
  private Properties clientArgs;

  public ThirdEyeClientConfig() {
    // cache enabled by default.
    this.cacheConfig = new CachedThirdEyeClientConfig();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getFactoryClassName() {
    return factoryClassName;
  }

  public void setFactoryClassName(String factoryClassName) {
    this.factoryClassName = factoryClassName;
  }

  public boolean isCached() {
    return this.getCacheConfig() != null;
  }

  public void setCached(boolean cached) {
    if (!cached) {
      this.cacheConfig = null;
    } else if (getCacheConfig() != null) {
      this.cacheConfig = new CachedThirdEyeClientConfig();
    }
  }

  public Properties getClientArgs() {
    return clientArgs;
  }

  public void setClientArgs(Map<String, String> map) throws IOException {
    Properties props = new Properties();
    props.putAll(map);
    this.clientArgs = props;
  }

  public CachedThirdEyeClientConfig getCacheConfig() {
    return cacheConfig;
  }

  public void setCacheConfig(CachedThirdEyeClientConfig cacheConfig) {
    this.cacheConfig = cacheConfig;
  }

  public ThirdEyeClient buildClient()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    ThirdEyeClientFactory factory =
        (ThirdEyeClientFactory) Class.forName(this.factoryClassName).newInstance();
    if (cacheConfig != null) {
      factory.configureCache(cacheConfig);
    }
    return factory.getClient(this.clientArgs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("factoryClass", factoryClassName)
        .add("cacheConfig", getCacheConfig()).add("props", clientArgs).toString();
  }

}
