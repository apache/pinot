package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestMetricStore
{
  private StarTreeConfig config;
  private MetricStoreImmutableImpl metricStore;
  private MetricSchema metricSchema;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    TimeRange timeRange = new TimeRange(0L, 3L);
    ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers =
        new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
    metricBuffers.put(timeRange, Arrays.asList(generateBuffer(timeRange)));
    metricStore = new MetricStoreImmutableImpl(config, metricBuffers);
  }

  @Test
  public void testGetTimeSeries_singleOffset_singleTime()
  {
    MetricTimeSeries timeSeries =
        metricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      1
    });
  }

  @Test
  public void testGetTimeSeries_singleOffset_multipleTime()
  {
    MetricTimeSeries timeSeries =
        metricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 1L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L, 1L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      2
    });
  }

  @Test
  public void testGetTimeSeries_multipleOffset_singleTime()
  {
    MetricTimeSeries timeSeries =
        metricStore.getTimeSeries(getLogicalOffsets(0, 1), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      2
    });
  }

  @Test
  public void testGetTimeSeries_multipleOffset_multipleTime()
  {
    MetricTimeSeries timeSeries =
        metricStore.getTimeSeries(getLogicalOffsets(0, 1), new TimeRange(0L, 1L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L, 1L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      4
    });
  }

  @Test
  public void testNotifyCreate()
  {
    TimeRange timeRange = new TimeRange(0L, 3L);
    ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers =
        new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
    metricBuffers.put(timeRange, Arrays.asList(generateBuffer(timeRange)));
    MetricStoreImmutableImpl createMetricStore =
        new MetricStoreImmutableImpl(config, metricBuffers);

    // Before (something)
    MetricTimeSeries timeSeries =
        createMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      1
    });
    timeSeries = createMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(4L, 4L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      0
    });

    timeRange = new TimeRange(4L, 7L);
    createMetricStore.notifyCreate(timeRange, generateBuffer(timeRange));

    // After (something)
    timeSeries = createMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      1
    });
    timeSeries = createMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(4L, 4L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      1
    });
  }

  @Test
  public void testNotifyDelete()
  {
    TimeRange timeRange = new TimeRange(0L, 3L);
    ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers =
        new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
    metricBuffers.put(timeRange, Arrays.asList(generateBuffer(timeRange)));
    MetricStoreImmutableImpl deleteMetricStore =
        new MetricStoreImmutableImpl(config, metricBuffers);

    // Before (something)
    MetricTimeSeries timeSeries =
        deleteMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      1
    });

    deleteMetricStore.notifyDelete(timeRange);

    // After (something)
    timeSeries = deleteMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] {
      0
    });
  }

  private List<Integer> getLogicalOffsets(Integer... offsets)
  {
    return Arrays.asList(offsets);
  }

  private Set<Long> getTimes(Long... times)
  {
    return new HashSet<Long>(Arrays.asList(times));
  }

  private List<MetricTimeSeries> generateTimeSeries(TimeRange timeRange)
  {
    List<MetricTimeSeries> result = new ArrayList<MetricTimeSeries>();

    for (int i = 0; i < 10; i++)
    {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

      // four hours (see sample-config.yml retention)
      for (long j = timeRange.getStart(); j <= timeRange.getEnd(); j++)
      {
        for (MetricSpec metricSpec : config.getMetrics())
        {
          timeSeries.set(j, metricSpec.getName(), 1);
        }
      }

      result.add(timeSeries);
    }

    return result;
  }

  private ByteBuffer generateBuffer(TimeRange timeRange)
  {
    List<MetricTimeSeries> allSeries = generateTimeSeries(timeRange);
    System.out.println(allSeries);
    ByteBuffer metricBuffer = VariableSizeBufferUtil.createMetricBuffer(config, allSeries);
    String dump = VariableSizeBufferUtil.dump(metricBuffer, metricSchema);
    System.out.println(dump);
    return metricBuffer;
  }
}
