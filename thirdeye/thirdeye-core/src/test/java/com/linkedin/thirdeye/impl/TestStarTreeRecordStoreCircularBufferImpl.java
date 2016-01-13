package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class TestStarTreeRecordStoreCircularBufferImpl {
  private final Map<String, Map<String, Integer>> forwardIndex =
      new HashMap<String, Map<String, Integer>>();
  private final int numTimeBuckets = 4;
  private final int numRecords = 100;

  private StarTreeConfig starTreeConfig;
  private MetricSchema metricSchema;
  private File rootDir;
  private UUID nodeId;
  private StarTreeRecordStoreFactory recordStoreFactory;
  private StarTreeRecordStore recordStore;

  @BeforeClass
  public void beforeClass() throws Exception {
    starTreeConfig =
        StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));

    metricSchema = MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics());

    rootDir = new File(System.getProperty("java.io.tmpdir"),
        TestStarTreeRecordStoreCircularBufferImpl.class.getSimpleName());

    nodeId = UUID.randomUUID();

    if (rootDir.exists()) {
      FileUtils.forceDelete(rootDir);
    }
    FileUtils.forceMkdir(rootDir);

    Map<String, Integer> aValues = new HashMap<String, Integer>();
    aValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
    aValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    aValues.put("A0", 2);
    aValues.put("A1", 3);
    aValues.put("A2", 4);
    aValues.put("A3", 5);
    aValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    aValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);

    Map<String, Integer> bValues = new HashMap<String, Integer>();
    bValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
    bValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    bValues.put("B0", 2);
    bValues.put("B1", 3);
    bValues.put("B2", 4);
    bValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    bValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);

    Map<String, Integer> cValues = new HashMap<String, Integer>();
    cValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
    cValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    cValues.put("C0", 2);
    cValues.put("C1", 3);
    cValues.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    cValues.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);

    forwardIndex.put("A", aValues);
    forwardIndex.put("B", bValues);
    forwardIndex.put("C", cValues);

    new ObjectMapper().writeValue(new File(rootDir, nodeId + StarTreeConstants.INDEX_FILE_SUFFIX),
        forwardIndex);

    Properties config = new Properties();
    recordStoreFactory = new StarTreeRecordStoreFactoryCircularBufferImpl();
    recordStoreFactory.init(rootDir, starTreeConfig, config);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    recordStore = recordStoreFactory.createRecordStore(nodeId);

    // Generate records
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    for (int i = 0; i < numRecords; i++) {
      DimensionKey dimensionKey = new DimensionKey(new String[] {
          "A" + (i % 4), "B" + (i % 3), "C" + (i % 2)
      });

      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
      timeSeries.set(i / (numRecords / numTimeBuckets), "M", 1);

      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
          .setDimensionKey(dimensionKey).setMetricTimeSeries(timeSeries);
      records.add(builder.build(starTreeConfig));
    }

    DimensionKey dimensionKey = new DimensionKey(new String[] {
        StarTreeConstants.OTHER, StarTreeConstants.OTHER, StarTreeConstants.OTHER
    });

    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
    timeSeries.set(0, "M", 0);

    // Add all-other record
    records.add(new StarTreeRecordImpl.Builder().setDimensionKey(dimensionKey)
        .setMetricTimeSeries(timeSeries).build(starTreeConfig));

    // Fill a buffer and write to bufferFile
    OutputStream outputStream =
        new FileOutputStream(new File(rootDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX));
    StarTreeRecordStoreCircularBufferImpl.fillBuffer(outputStream, starTreeConfig, forwardIndex,
        records, numTimeBuckets, true);
    outputStream.flush();
    outputStream.close();

    // Open
    recordStore.open();
  }

  @AfterClass
  public void afterClass() throws Exception {
    FileUtils.forceDelete(rootDir);
  }

  @Test
  public void testIterator() throws Exception {
    long sum = 0;

    for (StarTreeRecord record : recordStore) {
      for (Long timeWindow : record.getMetricTimeSeries().getTimeWindowSet()) {
        sum += record.getMetricTimeSeries().get(timeWindow, "M").intValue();
      }
    }

    Assert.assertEquals(sum, numRecords);
  }

  @Test
  public void testClear() throws Exception {
    recordStore.clear();

    long sum = 0;

    for (StarTreeRecord record : recordStore) {
      for (Long timeWindow : record.getMetricTimeSeries().getTimeWindowSet()) {
        sum += record.getMetricTimeSeries().get(timeWindow, "M").intValue();
      }
    }

    Assert.assertEquals(sum, 0);
  }

  @Test
  public void testGetMetricSums() throws Exception {
    // Specific
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("A0", "B0", "C0")).build(starTreeConfig);
    Number[] result = recordStore.getMetricSums(query);
    Assert.assertEquals(result[0], 4 + 3 + 2);

    // Aggregate
    query = new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "*", "*"))
        .build(starTreeConfig);
    result = recordStore.getMetricSums(query);
    Assert.assertEquals(result[0], numRecords);
  }

  @Test
  public void testGetTimeSeries() throws Exception {
    StarTreeQuery query =
        new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "*", "*"))
            .setTimeRange(new TimeRange(0L, 3L)).build(starTreeConfig);
    MetricTimeSeries timeSeries = recordStore.getTimeSeries(query);
    Assert.assertEquals(timeSeries.getTimeWindowSet().size(), 4);

    for (Long timeWindow : timeSeries.getTimeWindowSet()) {
      Assert.assertEquals(timeSeries.get(timeWindow, "M").intValue(), 25);
    }

    // No time range
    query = new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "*", "*"))
        .build(starTreeConfig);
    try {
      recordStore.getTimeSeries(query);
      Assert.fail();
    } catch (Exception e) {
      // Good
    }
  }

  @Test
  public void testLeastOtherMatch() throws Exception {
    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
    timeSeries.set(0, "M", 1);

    // This will go into all-other bucket because AX is unrecognized
    StarTreeRecord record =
        new StarTreeRecordImpl.Builder().setDimensionKey(getDimensionKey("AX", "B0", "C0"))
            .setMetricTimeSeries(timeSeries).build(starTreeConfig);

    recordStore.update(record);

    StarTreeQuery query = new StarTreeQueryImpl.Builder().setDimensionKey(
        getDimensionKey(StarTreeConstants.OTHER, StarTreeConstants.OTHER, StarTreeConstants.OTHER))
        .build(starTreeConfig);

    Number[] sums = recordStore.getMetricSums(query);
    Assert.assertEquals(sums[0], 1);
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
