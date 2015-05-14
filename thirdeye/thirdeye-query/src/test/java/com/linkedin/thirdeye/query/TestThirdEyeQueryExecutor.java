package com.linkedin.thirdeye.query;

import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestThirdEyeQueryExecutor {
  private StarTreeConfig config;
  private ExecutorService executorService;
  private ThirdEyeQueryExecutor queryExecutor;
  private DateTime start;
  private DateTime end;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("test-config.yml"));
    start = new DateTime(TimeUnit.MILLISECONDS.convert(0, TimeUnit.HOURS));
    end = new DateTime(TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));

    MetricSchema metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    StarTree starTree = new StarTreeImpl(config);
    starTree.open();
    for (int i = 0; i < 1024; i++) {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
      timeSeries.increment(i / 256, "M", 1);
      starTree.add(new StarTreeRecordImpl(config,
          new DimensionKey(new String[]{"A" + i % 2, "B" + i % 4, "C" + i % 8}),
          timeSeries));
    }

    PrintWriter printWriter = new PrintWriter(System.out);
    StarTreeUtils.printNode(printWriter, starTree.getRoot(), 0);
    printWriter.flush();

    StarTreeManager starTreeManager = mock(StarTreeManager.class);
    when(starTreeManager.getCollections()).thenReturn(ImmutableSet.of(config.getCollection()));
    when(starTreeManager.getConfig(config.getCollection())).thenReturn(config);
    when(starTreeManager.getStarTrees(config.getCollection())).thenReturn(ImmutableMap.of(new File("dummy"), starTree));
    when(starTreeManager.getIndexMetadata(starTree.getRoot().getId())).thenReturn(new IndexMetadata(0L, Long.MAX_VALUE));

    executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    queryExecutor = new ThirdEyeQueryExecutor(executorService, starTreeManager);
  }

  @AfterClass
  public void afterClass() {
    executorService.shutdownNow();
  }

  @Test
  public void testSimple_all() throws Exception {
    ThirdEyeQuery query = new ThirdEyeQuery()
        .setCollection(config.getCollection())
        .setStart(start)
        .setEnd(end);

    ThirdEyeQueryResult result = queryExecutor.executeQuery(query);

    Assert.assertEquals(result.getData().size(), 1);

    DimensionKey key = new DimensionKey(new String[] { "*", "*", "*" });
    MetricTimeSeries timeSeries = result.getData().get(key);
    Assert.assertNotNull(timeSeries);
    checkData(timeSeries, 256);
  }

  @Test
  public void testGroupBy_simple() throws Exception {
    ThirdEyeQuery query = new ThirdEyeQuery()
        .setCollection(config.getCollection())
        .setStart(start)
        .setEnd(end)
        .addGroupByColumn("A");

    ThirdEyeQueryResult result = queryExecutor.executeQuery(query);

    Assert.assertEquals(result.getData().size(), 3); // including other

    for (int i = 0; i < 2; i++) {
      DimensionKey key = new DimensionKey(new String[] { "A" + i, "*", "*" });
      Assert.assertNotNull(result.getData().get(key));
      checkData(result.getData().get(key), 128);
    }
  }

  @Test
  public void testGroupBy_includesOtherNode() throws Exception {
    ThirdEyeQuery query = new ThirdEyeQuery()
        .setCollection(config.getCollection())
        .setStart(start)
        .setEnd(end)
        .addGroupByColumn("B"); // tree has split on B

    ThirdEyeQueryResult result = queryExecutor.executeQuery(query);

    Assert.assertEquals(result.getData().size(), 5); // Including other category

    for (int i = 0; i < 4; i++) {
      DimensionKey key = new DimensionKey(new String[]{ "*", "B" + i, "*"});
      Assert.assertNotNull(result.getData().get(key));
      checkData(result.getData().get(key), 64);
    }

    DimensionKey otherKey = new DimensionKey(new String[] { "*", "?", "*" });
    Assert.assertNotNull(result.getData().get(otherKey));
    checkData(result.getData().get(otherKey), 0);
  }

  private void checkData(MetricTimeSeries timeSeries, long expectedValue) {
    for (Long time : timeSeries.getTimeWindowSet()) {
      for (int i = 0; i < timeSeries.getSchema().getNumMetrics(); i++) {
        Number metricValue = timeSeries.get(time, timeSeries.getSchema().getMetricName(i));
        Assert.assertTrue(metricValue instanceof Long);
        Assert.assertEquals(metricValue.longValue(), expectedValue);
      }
    }
  }
}
