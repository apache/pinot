package com.linkedin.thirdeye.client.factory;

import java.util.Properties;

import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.util.ThirdEyeClientConfig;

/**
 * Factory interface for creating {@link ThirdEyeClient} instances from property files, using
 * predefined caching configurations. Useful for creating clients from files, eg as in
 * {@link ThirdEyeClientConfig}.
 * @author jteoh
 */
public interface ThirdEyeClientFactory {
  public ThirdEyeClient getClient(Properties props);

  public ThirdEyeClientFactory configureCache(CachedThirdEyeClientConfig config);
}
