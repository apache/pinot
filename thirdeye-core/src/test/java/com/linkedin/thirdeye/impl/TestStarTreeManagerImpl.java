package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

public class TestStarTreeManagerImpl
{
  private StarTreeManager starTreeManager;
  private StarTreeConfig config;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    List<String> dimensionNames = Arrays.asList("A", "B", "C");
    List<String> metricNames = Arrays.asList("M");
    starTreeManager = new StarTreeManagerImpl(Executors.newSingleThreadExecutor());
    config = new StarTreeConfig.Builder()
            .setMetricNames(metricNames)
            .setDimensionNames(dimensionNames)
            .build();
  }

  @Test
  public void testRegisterConfig() throws Exception
  {
    starTreeManager.registerConfig("myCollection", config);
    Assert.assertNotNull(starTreeManager.getConfig("myCollection"));
    Assert.assertNull(starTreeManager.getConfig("yourCollection"));
  }

  @Test
  public void testLoad() throws Exception
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();

    for (int i = 0; i < 10; i++)
    {
      records.add(new StarTreeRecordImpl.Builder()
                          .setDimensionValue("A", "A" + (i % 2))
                          .setDimensionValue("B", "B" + (i % 4))
                          .setDimensionValue("C", "C" + (i % 8))
                          .setMetricValue("M", 1L)
                          .setTime(System.currentTimeMillis())
                          .build());
    }

    starTreeManager.registerConfig("myCollection", config);
    starTreeManager.load("myCollection", records);

    StarTree starTree = starTreeManager.getStarTree("myCollection");
    Assert.assertNotNull(starTree);

    StarTreeRecord result =
            starTree.search(new StarTreeQueryImpl.Builder()
                                    .setDimensionValue("A", "*")
                                    .setDimensionValue("B", "*")
                                    .setDimensionValue("C", "*")
                                    .build());

    Assert.assertNotNull(result);
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 10);
  }
}
