package com.linkedin.thirdeye.client.util;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.factory.DefaultThirdEyeClientFactory;
import com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory;
import com.linkedin.thirdeye.client.factory.PinotThirdEyeClientFactory;

public class TestThirdEyeClientConfig {
  private static final String MOCK_CLIENT_FACTORY_NAME = MockThirdEyeClientFactory.class.getName();
  private static final String PINOT_CLIENT_FACTORY_NAME =
      PinotThirdEyeClientFactory.class.getName();
  private static final String DEFAULT_CLIENT_FACTORY_NAME =
      DefaultThirdEyeClientFactory.class.getName();

  private static final CachedThirdEyeClientConfig DEFAULT_CACHE_CONFIG =
      new CachedThirdEyeClientConfig();

  @Test
  public void fromSampleFile() throws Exception {
    ObjectMapper ymlReader = new ObjectMapper(new YAMLFactory());
    InputStream resource = ClassLoader.getSystemResourceAsStream("test-clients.yml");
    List<ThirdEyeClientConfig> clients = ymlReader.readValue(resource,
        ymlReader.getTypeFactory().constructCollectionType(List.class, ThirdEyeClientConfig.class));
    // see sample file for expected configs.
    ThirdEyeClientConfig mockClientConfig = clients.get(0);
    Assert.assertEquals(mockClientConfig.getName(), "mockClient");
    Assert.assertEquals(mockClientConfig.getFactoryClassName(), MOCK_CLIENT_FACTORY_NAME);
    Assert.assertTrue(mockClientConfig.isCached());
    Assert.assertNotNull(mockClientConfig.buildClient());

    ThirdEyeClientConfig pinotCacheDisabledConfig = clients.get(1);
    Assert.assertEquals(pinotCacheDisabledConfig.getName(), "pinotCacheDisabledConfig");
    Assert.assertEquals(pinotCacheDisabledConfig.getFactoryClassName(), PINOT_CLIENT_FACTORY_NAME);
    Assert.assertFalse(pinotCacheDisabledConfig.isCached());
    Assert.assertNotNull(pinotCacheDisabledConfig.buildClient());

    ThirdEyeClientConfig defaultCacheEnabledConfig = clients.get(2);
    Assert.assertEquals(defaultCacheEnabledConfig.getName(), "defaultCacheEnabledConfig");
    Assert.assertEquals(defaultCacheEnabledConfig.getFactoryClassName(),
        DEFAULT_CLIENT_FACTORY_NAME);
    Assert.assertTrue(defaultCacheEnabledConfig.isCached());
    Assert.assertEquals(defaultCacheEnabledConfig.getCacheConfig(), DEFAULT_CACHE_CONFIG);
    Assert.assertNotNull(defaultCacheEnabledConfig.buildClient());

    ThirdEyeClientConfig customCacheConfigConfig = clients.get(3);
    Assert.assertEquals(customCacheConfigConfig.getName(), "customCacheConfigConfig");
    Assert.assertEquals(customCacheConfigConfig.getFactoryClassName(), DEFAULT_CLIENT_FACTORY_NAME);
    Assert.assertTrue(customCacheConfigConfig.isCached());
    CachedThirdEyeClientConfig customCacheConfig = customCacheConfigConfig.getCacheConfig();
    Assert.assertNotEquals(customCacheConfig, DEFAULT_CACHE_CONFIG);
    Assert.assertEquals(customCacheConfig.isExpireAfterAccess(), false);
    Assert.assertEquals(customCacheConfig.getExpirationTime(), 180);
    Assert.assertEquals(customCacheConfig.getExpirationUnit(), TimeUnit.MINUTES);
    Assert.assertEquals(customCacheConfig.useCacheForExecuteMethod(), false);
    Assert.assertNotNull(customCacheConfigConfig.buildClient());

    ThirdEyeClientConfig cacheEnabledConfig = clients.get(4);
    Assert.assertEquals(cacheEnabledConfig.getName(), "cacheEnabledConfig");
    Assert.assertEquals(defaultCacheEnabledConfig.getFactoryClassName(),
        DEFAULT_CLIENT_FACTORY_NAME);
    Assert.assertTrue(cacheEnabledConfig.isCached());
    Assert.assertEquals(cacheEnabledConfig.getCacheConfig(), DEFAULT_CACHE_CONFIG);
    Assert.assertNotNull(cacheEnabledConfig.buildClient());
  }
}
