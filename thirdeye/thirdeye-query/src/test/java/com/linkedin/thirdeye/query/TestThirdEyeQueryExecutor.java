package com.linkedin.thirdeye.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.IndexFormat;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

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
    when(starTreeManager.getIndexMetadata(starTree.getRoot().getId())).thenReturn
    (new IndexMetadata(0L, Long.MAX_VALUE, 0L, Long.MAX_VALUE,
        0L, Long.MAX_VALUE, 0L, Long.MAX_VALUE,
        "HOURLY", TimeUnit.HOURS.toString(), 1, IndexFormat.VARIABLE_SIZE));

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

    System.out.println(result.getData());

    Assert.assertEquals(result.getData().size(), 3); // including other

    for (int i = 0; i < 2; i++) {
      DimensionKey key = new DimensionKey(new String[] { "A" + i, "*", "*" });
      Assert.assertNotNull(result.getData().get(key));
      checkData(result.getData().get(key), 128);
    }
  }

  @Test
  public void testOr_simple() throws Exception {
    ThirdEyeQuery query = new ThirdEyeQuery()
        .setCollection(config.getCollection())
        .setStart(start)
        .setEnd(end)
        .addDimensionValue("A", "A0")
        .addDimensionValue("A", "A1");

    ThirdEyeQueryResult result = queryExecutor.executeQuery(query);

    Assert.assertEquals(result.getData().size(), 1);

    DimensionKey key = new DimensionKey(new String[] { "A0 OR A1", "*", "*" });
    Assert.assertNotNull(result.getData().get(key));
    checkData(result.getData().get(key), 256); // A can be A0 or A1, so all
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

  @Test
  public void testSelectTreeForQueryTimeRange_alignedDataTime() throws Exception {
    Object[][] indexMetadata = {
        // MONTHLY
        { UUID.randomUUID(), createIndexMetadata("2014-10TZ", "2014-11TZ", "2014-10TZ", "2014-11TZ", "MONTHLY") },
        { UUID.randomUUID(), createIndexMetadata("2014-11TZ", "2014-12TZ", "2014-11TZ", "2014-12TZ", "MONTHLY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12TZ", "2015-01TZ", "2014-12TZ", "2015-01TZ", "MONTHLY") }, // use
        // DAILY
        { UUID.randomUUID(), createIndexMetadata("2014-12-24TZ", "2014-12-25TZ", "2014-12-24TZ", "2014-12-25TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-25TZ", "2014-12-26TZ", "2014-12-25TZ", "2014-12-26TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-26TZ", "2014-12-27TZ", "2014-12-26TZ", "2014-12-27TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-27TZ", "2014-12-28TZ", "2014-12-27TZ", "2014-12-28TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-28TZ", "2014-12-29TZ", "2014-12-28TZ", "2014-12-29TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-29TZ", "2014-12-30TZ", "2014-12-29TZ", "2014-12-30TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2014-12-30TZ", "2014-12-31TZ", "2014-12-30TZ", "2014-12-31TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2015-01-01TZ", "2015-01-02TZ", "2015-01-01TZ", "2015-01-02TZ", "DAILY") }, // use
        { UUID.randomUUID(), createIndexMetadata("2015-01-02TZ", "2015-01-03TZ", "2015-01-02TZ", "2015-01-03TZ", "DAILY") }, // use
        { UUID.randomUUID(), createIndexMetadata("2015-01-03TZ", "2015-01-04TZ", "2015-01-03TZ", "2015-01-04TZ", "DAILY") }, // use
        { UUID.randomUUID(), createIndexMetadata("2015-01-04TZ", "2015-01-05TZ", "2015-01-04TZ", "2015-01-05TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2015-01-05TZ", "2015-01-06TZ", "2015-01-05TZ", "2015-01-06TZ", "DAILY") },
        { UUID.randomUUID(), createIndexMetadata("2015-01-06TZ", "2015-01-07TZ", "2015-01-06TZ", "2015-01-07TZ", "DAILY") },
    };

    Map<UUID, IndexMetadata> indexMetadataMap = new HashMap<>();
    for (Object[] datum : indexMetadata) {
      indexMetadataMap.put((UUID) datum[0], (IndexMetadata) datum[1]);
    }

    DateTime queryStartTime = ISODateTimeFormat.dateTimeParser().parseDateTime("2014-12-02TZ").toDateTime(DateTimeZone.UTC);
    DateTime queryEndTime = ISODateTimeFormat.dateTimeParser().parseDateTime("2015-01-04TZ").toDateTime(DateTimeZone.UTC);
    TimeRange queryTimeRange = new TimeRange(queryStartTime.getMillis(), queryEndTime.getMillis());

    List<UUID> ids = queryExecutor.selectTreesToQuery(indexMetadataMap, queryTimeRange);

    Assert.assertEquals(ids, ImmutableList.of(
        (UUID) indexMetadata[2][0],
        (UUID) indexMetadata[10][0],
        (UUID) indexMetadata[11][0],
        (UUID) indexMetadata[12][0],
        (UUID) indexMetadata[13][0]
    ));
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

  private IndexMetadata createIndexMetadata(String startWallTime,
                                            String endWallTime,
                                            String startDataTime,
                                            String endDataTime,
                                            String timeGranularity) {
    DateTime startWall = ISODateTimeFormat.dateTimeParser().parseDateTime(startWallTime).toDateTime(DateTimeZone.UTC);
    DateTime endWall = ISODateTimeFormat.dateTimeParser().parseDateTime(endWallTime).toDateTime(DateTimeZone.UTC);
    DateTime startData = ISODateTimeFormat.dateTimeParser().parseDateTime(startDataTime).toDateTime(DateTimeZone.UTC);
    DateTime endData = ISODateTimeFormat.dateTimeParser().parseDateTime(endDataTime).toDateTime(DateTimeZone.UTC);

    return new IndexMetadata(startData.getMillis(),
        endData.getMillis(),
        startData.getMillis(),
        endData.getMillis(),
        startWall.getMillis(),
        endWall.getMillis(),
        startWall.getMillis(),
        endWall.getMillis(),
        timeGranularity,
        TimeUnit.MILLISECONDS.toString(),
        1,
        IndexFormat.VARIABLE_SIZE);
  }
}
