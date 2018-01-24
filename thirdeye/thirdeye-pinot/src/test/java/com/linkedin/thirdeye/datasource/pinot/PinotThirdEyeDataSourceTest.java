package com.linkedin.thirdeye.datasource.pinot;

import com.linkedin.thirdeye.constant.MetricAggFunction;
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

  @Test
  public void testReduceSum() {
    Assert.assertEquals(PinotThirdEyeDataSource.reduce(10, 3, 4, MetricAggFunction.SUM), 13.0);
  }

  @Test
  public void testReduceAvg() {
    Assert.assertEquals(PinotThirdEyeDataSource.reduce(10, 2, 3, MetricAggFunction.AVG), 8.0);
  }

  @Test
  public void testReduceMax() {
    Assert.assertEquals(PinotThirdEyeDataSource.reduce(10, 3, 12, MetricAggFunction.MAX), 10.0);
  }

  @Test
  public void testReduceCount() {
    Assert.assertEquals(PinotThirdEyeDataSource.reduce(4, 3, 4, MetricAggFunction.COUNT), 5.0);
  }

}
