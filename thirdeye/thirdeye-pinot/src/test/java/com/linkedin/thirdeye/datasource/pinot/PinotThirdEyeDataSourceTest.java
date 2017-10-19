package com.linkedin.thirdeye.datasource.pinot;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PinotThirdEyeDataSourceTest {

  @Test
  public void testInitializeCacheLoaderFromGivenClass() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(PinotThirdEyeDataSource.CACHE_LOADER_CLASS_NAME_STRING,
        PinotControllerResponseCacheLoader.class.getName());

    PinotResponseCacheLoader cacheLoaderInstance = PinotThirdEyeDataSource.getCacheLoaderInstance(properties);
    Assert.assertTrue(PinotControllerResponseCacheLoader.class.equals(cacheLoaderInstance.getClass()));
  }

  @Test
  public void testInitializeCacheLoaderFromEmptyClass() throws Exception {
    Map<String, String> properties = Collections.emptyMap();

    PinotResponseCacheLoader cacheLoaderInstance = PinotThirdEyeDataSource.getCacheLoaderInstance(properties);
    Assert.assertTrue(PinotControllerResponseCacheLoader.class.equals(cacheLoaderInstance.getClass()));
  }

}
