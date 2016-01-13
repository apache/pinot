package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.TimeRange;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TestStarTreeRecordStoreLogBufferImpl {
  private StarTreeConfig config;
  private MetricSchema metricSchema;

  private List<StarTreeRecordStore> recordStores;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    recordStores = new ArrayList<StarTreeRecordStore>();

    StarTreeRecordStore bufferStore =
        new StarTreeRecordStoreLogBufferImpl(UUID.randomUUID(), config, 1024, true, 0.8);
    bufferStore.open();
    recordStores.add(bufferStore);
  }

  @AfterClass
  public void afterClass() throws Exception {
    for (StarTreeRecordStore recordStore : recordStores) {
      recordStore.close();
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    for (StarTreeRecordStore recordStore : recordStores) {
      recordStore.clear();
    }
  }

  @DataProvider
  public Object[][] recordStoreDataProvider() {
    Object[][] objects = new Object[recordStores.size()][];
    int idx = 0;
    for (StarTreeRecordStore recordStore : recordStores) {
      objects[idx++] = new Object[] {
          recordStore
      };
    }
    return objects;
  }

  @Test(dataProvider = "recordStoreDataProvider")
  public void testSameUpdate(StarTreeRecordStore recordStore) throws Exception {
    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
    timeSeries.set(0, "M", 1000);

    StarTreeRecord record =
        new StarTreeRecordImpl.Builder().setDimensionKey(getDimensionKey("A1", "B1", "C1"))
            .setMetricTimeSeries(timeSeries).build(config);

    // Add one
    recordStore.update(record);

    // Check it's the only one
    Iterator<StarTreeRecord> itr = recordStore.iterator();
    StarTreeRecord fromStore = itr.next();
    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(fromStore.getMetricTimeSeries().get(0, "M").intValue(), 1000);

    // Add the same one
    recordStore.update(record);

    // Check if there's two now
    itr = recordStore.iterator(); // this compacts the buffer, so should only be one
    Assert.assertNotNull(itr.next());
    Assert.assertFalse(itr.hasNext());
  }

  @Test(dataProvider = "recordStoreDataProvider")
  public void testMutuallyExclusiveUpdate(StarTreeRecordStore recordStore) throws Exception {
    MetricTimeSeries ts1 = new MetricTimeSeries(metricSchema);
    ts1.set(0, "M", 1000);

    StarTreeRecord first = new StarTreeRecordImpl.Builder()
        .setDimensionKey(getDimensionKey("A1", "B1", "C1")).setMetricTimeSeries(ts1).build(config);

    recordStore.update(first);

    MetricTimeSeries ts2 = new MetricTimeSeries(metricSchema);
    ts2.set(0, "M", 1000);

    StarTreeRecord second = new StarTreeRecordImpl.Builder()
        .setDimensionKey(getDimensionKey("A2", "B2", "C2")).setMetricTimeSeries(ts2).build(config);

    recordStore.update(second);

    for (StarTreeRecord record : recordStore) {
      System.out.println(record);
    }

    Iterator<StarTreeRecord> itr = recordStore.iterator();
    Set<StarTreeRecord> fromStore = new HashSet<StarTreeRecord>();
    fromStore.add(itr.next());
    fromStore.add(itr.next());
    Assert.assertFalse(itr.hasNext());
    Assert.assertTrue(fromStore.contains(first));
    Assert.assertTrue(fromStore.contains(second));
  }

  @Test(dataProvider = "recordStoreDataProvider")
  public void testGetTimeSeries(StarTreeRecordStore recordStore) throws Exception {
    // Add some time series data
    for (int i = 0; i < 100; i++) {
      MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
      ts.set(i / 25, "M", 1000);

      StarTreeRecord record = new StarTreeRecordImpl.Builder()
          .setDimensionKey(getDimensionKey("A1", "B1", "C1")).setMetricTimeSeries(ts).build(config);

      recordStore.update(record);
    }

    // Time range query
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("*", "*", "*")).setTimeRange(new TimeRange(0L, 4L)) // 4
                                                                                             // won't
                                                                                             // be
                                                                                             // included
        .build(config);

    // Ensure we've got 4 time series elements (0,1,2,3)
    MetricTimeSeries timeSeries = recordStore.getTimeSeries(query);
    Assert.assertEquals(timeSeries.getTimeWindowSet().size(), 4);

    // No time, so should fail
    query = new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "*", "*"))
        .build(config);
    try {
      recordStore.getTimeSeries(query);
      Assert.fail();
    } catch (Exception e) {
      // Good
    }
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
