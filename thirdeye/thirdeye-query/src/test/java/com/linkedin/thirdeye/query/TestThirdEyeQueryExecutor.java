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

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
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
        "HOURLY", TimeUnit.HOURS.toString(), 1));

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

  @Test
  public void testSelectTreeForQueryTimeRange() throws Exception {
    String[][] indexMetadataArray = new String[][] {
        new String[] {
            "2014-10", "2014-11", "MONTHLY"
        }, //
        new String[] {
            "2014-11", "2014-12", "MONTHLY"
        }, //
        new String[] {
            "2014-12", "2015-01", "MONTHLY"
        }, //
        new String[] {
            "2014-12-24", "2014-12-25", "DAILY"
        }, //
        new String[] {
            "2014-12-25", "2014-12-26", "DAILY"
        }, //
        new String[] {
            "2014-12-26", "2014-12-27", "DAILY"
        }, //
        new String[] {
            "2014-12-27", "2014-12-28", "DAILY"
        }, //
        new String[] {
            "2014-12-28", "2014-12-29", "DAILY"
        }, //
        new String[] {
            "2014-12-29", "2014-12-30", "DAILY"
        }, //
        new String[] {
            "2014-12-30", "2014-12-31", "DAILY"
        }, //
        new String[] {
            "2015-01-01", "2015-01-02", "DAILY"
        }, //
        new String[] {
            "2015-01-02", "2015-01-03", "DAILY"
        }, //
        new String[] {
            "2015-01-03", "2015-01-04", "DAILY"
        }, //
        new String[] {
            "2015-01-04", "2015-01-05", "DAILY"
        }, //
        new String[] {
            "2015-01-05", "2015-01-06", "DAILY"
        }, //
        new String[] {
            "2015-01-06", "2015-01-07", "DAILY"
        }
    //
        // new String[]{"2015-01-07 00:00:00", "2015-01-07", "HOURLY"}, //

        };
    Map<UUID, IndexMetadata> treeMetadataMap = new HashMap<UUID, IndexMetadata>();
    Map<UUID, Integer> treeIdToArrayIndexMapping = new HashMap<UUID, Integer>();

    for (int i = 0; i < indexMetadataArray.length; i++) {
      Long minDataTimeMillis = toMilliSecond(indexMetadataArray[i][0], indexMetadataArray[i][2]);
      Long maxDataTimeMillis = toMilliSecond(indexMetadataArray[i][1], indexMetadataArray[i][2]);
      Long startTimeMillis = toMilliSecond(indexMetadataArray[i][0], indexMetadataArray[i][2]);
      Long endTimeMillis = toMilliSecond(indexMetadataArray[i][1], indexMetadataArray[i][2]);
      String timeGranularity = indexMetadataArray[i][2];

      TimeUnit aggregationGranularity = TimeUnit.HOURS;
      int bucketSize = 1;

      Long minDataTime = aggregationGranularity.convert(minDataTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;
      Long maxDataTime = aggregationGranularity.convert(maxDataTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;
      Long startTime = aggregationGranularity.convert(startTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;
      Long endTime = aggregationGranularity.convert(endTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;

      IndexMetadata value =
          new IndexMetadata(minDataTime, maxDataTime, minDataTimeMillis, maxDataTimeMillis,
              startTime, endTime, startTimeMillis, endTimeMillis,
              timeGranularity, aggregationGranularity.toString(), bucketSize);
      UUID uuid = UUID.nameUUIDFromBytes(("" + i).getBytes());
      System.out.println("Adding metadata for treeId:" + uuid
          + Arrays.toString(indexMetadataArray[i]) + " metadata:" + value);
      treeMetadataMap.put(uuid, value);
      treeIdToArrayIndexMapping.put(uuid, i);
    }
    String queryStart = "2014-12-02";
    String queryEnd = "2015-01-04";
    TimeRange queryTimeRange =
        new TimeRange(toMilliSecond(queryStart, "DAILY"), toMilliSecond(queryEnd, "DAILY"));
    System.out.println(queryTimeRange);
    List<UUID> treesToQuery = queryExecutor.selectTreesToQuery(treeMetadataMap, queryTimeRange);
    System.out.println(treesToQuery);
    System.out.println("The following tree's will be queried for query range " + (queryStart)
        + " - " + queryEnd);
    Assert.assertEquals(treesToQuery.size(), 4);
    for (UUID treeId : treesToQuery) {
      String[] metadataInfo = indexMetadataArray[treeIdToArrayIndexMapping.get(treeId)];
      System.out.println(treeId + ":" + Arrays.toString(metadataInfo));
    }
    Assert.assertEquals(indexMetadataArray[treeIdToArrayIndexMapping.get(treesToQuery.get(0))],
        new String[] {
            "2014-12", "2015-01", "MONTHLY"
        });
    Assert.assertEquals(indexMetadataArray[treeIdToArrayIndexMapping.get(treesToQuery.get(1))],
        new String[] {
            "2015-01-01", "2015-01-02", "DAILY"
        });
    Assert.assertEquals(indexMetadataArray[treeIdToArrayIndexMapping.get(treesToQuery.get(2))],
        new String[] {
            "2015-01-02", "2015-01-03", "DAILY"
        });
    Assert.assertEquals(indexMetadataArray[treeIdToArrayIndexMapping.get(treesToQuery.get(3))],
        new String[] {
            "2015-01-03", "2015-01-04", "DAILY"
        });

  }

  private long toMilliSecond(String dateString, String granularity) throws Exception {
    long ret;
    if (granularity.equalsIgnoreCase("MONTHLY")) {
      ret = new SimpleDateFormat("yyyy-MM").parse(dateString).getTime();
    } else if (granularity.equalsIgnoreCase("DAILY")) {
      ret = new SimpleDateFormat("yyyy-MM-dd").parse(dateString).getTime();
    } else if (granularity.equalsIgnoreCase("HOURLY")) {
      ret = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateString).getTime();
    } else {
      throw new Exception("Unsupported granularity: " + granularity);
    }
    System.out.println("Converted "+ dateString + " of granularity:"+ granularity + " to " + ret + " ms to timestamp:"+ new Timestamp(ret));
    return ret;
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
