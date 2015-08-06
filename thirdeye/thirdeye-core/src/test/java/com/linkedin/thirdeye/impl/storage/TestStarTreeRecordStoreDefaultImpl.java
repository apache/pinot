package com.linkedin.thirdeye.impl.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestStarTreeRecordStoreDefaultImpl
{
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private DimensionDictionary dictionary;
  private DimensionStoreImmutableImpl dimensionStore;
  private MetricStoreImmutableImpl metricStore;
  private StarTreeRecordStore recordStore;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-dictionary.json");
    dictionary = objectMapper.readValue(inputStream, DimensionDictionary.class);
    inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    // Dimensions
    ByteBuffer dimensionBuffer = ByteBuffer.allocate(1024 * 1024);
    for (DimensionKey key : generateKeys())
    {
      StorageUtils.addToDimensionStore(config, dimensionBuffer, key, dictionary);
    }
    dimensionBuffer.flip();
    dimensionStore = new DimensionStoreImmutableImpl(config, dimensionBuffer, dictionary);

    // Metrics
    List<TimeRange> timeRanges = Arrays.asList(new TimeRange(0L, 3L), new TimeRange(4L, 7L));
    ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers = new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
    for (TimeRange timeRange : timeRanges)
    {
      metricBuffers.put(timeRange, Arrays.asList(generateBuffer(timeRange)));
    }
    metricStore = new MetricStoreImmutableImpl(config, metricBuffers);

    // Record store
    recordStore = new StarTreeRecordStoreDefaultImpl(config, dimensionStore, metricStore);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUpdate()
  {
    MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
    ts.set(0, "M", 1);

    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionKey(getDimensionKey("A0", "B0", "C0"))
            .setMetricTimeSeries(ts)
            .build(config);

    recordStore.update(record);
  }

  @Test
  public void testGetRecordCount()
  {
    Assert.assertEquals(recordStore.getRecordCount(), 10);
  }

  @Test
  public void testGetRecordCountEstimate()
  {
    Assert.assertEquals(recordStore.getRecordCountEstimate(), 10);
  }

  @Test
  public void testGetCardinality()
  {
    Assert.assertEquals(recordStore.getCardinality("A"), 3);
    Assert.assertEquals(recordStore.getCardinality("B"), 6);
    Assert.assertEquals(recordStore.getCardinality("C"), 9);
  }

  @Test
  public void testGetMaxCardinalityDimension()
  {
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(), "C");
  }

  @Test
  public void testGetMaxCardinalityDimension_withBlacklist()
  {
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(Arrays.asList("C")), "B");
  }

  @Test
  public void testGetDimensionValues()
  {
    Assert.assertEquals(recordStore.getDimensionValues("A"), new HashSet<String>(Arrays.asList("A0", "A1", "A2")));
  }

  @Test
  public void testGetMinTime()
  {
    Assert.assertEquals(recordStore.getMinTime(), Long.valueOf(0L));
  }

  @Test
  public void testGetMaxTime()
  {
    Assert.assertEquals(recordStore.getMaxTime(), Long.valueOf(7L));
  }

  @Test
  public void testGetMetricSums_oneSegment_allStar()
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey(StarTreeConstants.STAR, StarTreeConstants.STAR, StarTreeConstants.STAR))
            .setTimeRange(new TimeRange(0L, 0L))
            .build(config);

    Number[] sums = recordStore.getMetricSums(query);

    Assert.assertEquals(sums, getSums(10));
  }

  @Test
  public void testGetMetricSums_twoSegment_allStar()
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey(StarTreeConstants.STAR, StarTreeConstants.STAR, StarTreeConstants.STAR))
            .setTimeRange(new TimeRange(0L, 7L))
            .build(config);

    Number[] sums = recordStore.getMetricSums(query);

    Assert.assertEquals(sums, getSums(80));
  }

  @Test
  public void testGetMetricSums_oneSegment_someStar()
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey("A0", StarTreeConstants.STAR, StarTreeConstants.STAR))
            .setTimeRange(new TimeRange(0L, 0L))
            .build(config);

    Number[] sums = recordStore.getMetricSums(query);

    Assert.assertEquals(sums, getSums(4));
  }

  @Test
  public void testGetMetricSums_twoSegment_someStar()
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey("A0", StarTreeConstants.STAR, StarTreeConstants.STAR))
            .setTimeRange(new TimeRange(0L, 7L))
            .build(config);

    Number[] sums = recordStore.getMetricSums(query);

    Assert.assertEquals(sums, getSums(8 * 4));
  }

  @Test
  public void testGetTimeSeries_twoSegments_allStars()
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey(StarTreeConstants.STAR, StarTreeConstants.STAR, StarTreeConstants.STAR))
            .setTimeRange(new TimeRange(0L, 7L))
            .build(config);

    MetricTimeSeries timeSeries = recordStore.getTimeSeries(query);

    Assert.assertEquals(timeSeries.getTimeWindowSet().size(), 8);

    for (Long time : timeSeries.getTimeWindowSet())
    {
      Assert.assertEquals(timeSeries.get(time, "M").intValue(), 10);
    }
  }

  private static List<DimensionKey> generateKeys()
  {
    List<DimensionKey> keys = new ArrayList<DimensionKey>();

    for (int i = 0; i < 10; i++)
    {
      DimensionKey key = new DimensionKey(new String[]{
              "A" + (i % 3),
              "B" + (i % 6),
              "C" + (i % 9)
      });

      keys.add(key);
    }

    return keys;
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

  private ByteBuffer generateBuffer(TimeRange timeRange)
  {
    List<MetricTimeSeries> allSeries = generateTimeSeries(timeRange);
    // System.out.println(allSeries);
    ByteBuffer metricBuffer = VariableSizeBufferUtil.createMetricBuffer(config, allSeries);
    String dump = VariableSizeBufferUtil.dump(metricBuffer, metricSchema);
    // System.out.println(dump);
    return metricBuffer;
  }

  private Number[] getSums(Number m)
  {
    return new Number[]{m};
  }

  private DimensionKey getDimensionKey(String a, String b, String c)
  {
    return new DimensionKey(new String[] {a, b, c});
  }
}
