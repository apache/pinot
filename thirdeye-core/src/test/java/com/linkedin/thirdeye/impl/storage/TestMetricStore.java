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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestMetricStore
{
  private StarTreeConfig config;
  private MetricStore metricStore;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);
    metricStore = new MetricStore(config, generateBuffer(new TimeRange(0L, 3L)));
  }

  @Test
  public void testGetTimeSeries_singleOffset_singleTime()
  {
    MetricTimeSeries timeSeries = metricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[]{1});
  }

  @Test
  public void testGetTimeSeries_singleOffset_multipleTime()
  {
    MetricTimeSeries timeSeries = metricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 1L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L, 1L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] { 2 });
  }

  @Test
  public void testGetTimeSeries_multipleOffset_singleTime()
  {
    MetricTimeSeries timeSeries = metricStore.getTimeSeries(getLogicalOffsets(0, 1), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] { 2 });
  }

  @Test
  public void testGetTimeSeries_multipleOffset_multipleTime()
  {
    MetricTimeSeries timeSeries = metricStore.getTimeSeries(getLogicalOffsets(0, 1), new TimeRange(0L, 1L));
    Assert.assertEquals(timeSeries.getTimeWindowSet(), getTimes(0L, 1L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[] { 4 });
  }

  @Test
  public void testRefresh()
  {
    MetricStore refreshMetricStore = new MetricStore(config, generateBuffer(new TimeRange(0L, 3L)));

    // Before (something)
    MetricTimeSeries timeSeries = refreshMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[]{1});

    refreshMetricStore.refresh(generateBuffer(new TimeRange(4L, 7L)));

    // After (nothing)
    timeSeries = refreshMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(0L, 0L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[]{0});

    // After (something)
    timeSeries = refreshMetricStore.getTimeSeries(getLogicalOffsets(0), new TimeRange(4L, 4L));
    Assert.assertEquals(timeSeries.getMetricSums(), new Number[]{1});
  }

  private Set<Integer> getLogicalOffsets(Integer... offsets)
  {
    return new HashSet<Integer>(Arrays.asList(offsets));
  }

  private Set<Long> getTimes(Long... times)
  {
    return new HashSet<Long>(Arrays.asList(times));
  }

  private List<MetricTimeSeries> generateTimeSeries(TimeRange timeRange)
  {
    List<MetricTimeSeries> result = new ArrayList<MetricTimeSeries>();

    MetricSchema metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

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

  private Map<TimeRange, ByteBuffer> generateBuffer(TimeRange timeRange)
  {
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    List<MetricTimeSeries> allSeries = generateTimeSeries(timeRange);

    for (MetricTimeSeries timeSeries : allSeries)
    {
      StorageUtils.addToMetricStore(config, buffer, timeSeries);
    }

    buffer.flip();

    return Collections.singletonMap(timeRange, buffer);
  }
}
