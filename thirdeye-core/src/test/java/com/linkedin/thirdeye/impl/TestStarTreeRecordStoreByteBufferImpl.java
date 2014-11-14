package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class TestStarTreeRecordStoreByteBufferImpl
{
  private final List<String> dimensionNames = Arrays.asList("A", "B", "C");
  private final List<String> metricNames = Arrays.asList("M");

  private List<StarTreeRecordStore> recordStores;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    recordStores = new ArrayList<StarTreeRecordStore>();

    StarTreeRecordStore bufferStore = new StarTreeRecordStoreLogBufferImpl(UUID.randomUUID(), dimensionNames, metricNames, 1024, true, 0.8);
    bufferStore.open();
    recordStores.add(bufferStore);
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    for (StarTreeRecordStore recordStore : recordStores)
    {
      recordStore.close();
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    for (StarTreeRecordStore recordStore : recordStores)
    {
      recordStore.clear();
    }
  }

  @DataProvider
  public Object[][] recordStoreDataProvider()
  {
    Object[][] objects = new Object[recordStores.size()][];
    int idx = 0;
    for (StarTreeRecordStore recordStore : recordStores)
    {
      objects[idx++] = new Object[]{ recordStore };
    }
    return objects;
  }

  @Test(dataProvider = "recordStoreDataProvider")
  public void testSameUpdate(StarTreeRecordStore recordStore) throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 1000)
            .build();

    // Add one
    recordStore.update(record);

    // Check it's the only one
    Iterator<StarTreeRecord> itr = recordStore.iterator();
    StarTreeRecord fromStore = itr.next();
    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(fromStore.getMetricValues().get("M").intValue(), 1000);

    // Add the same one
    recordStore.update(record);

    // Check if there's two now
    itr = recordStore.iterator();
    Assert.assertNotNull(itr.next());
    Assert.assertNotNull(itr.next());
  }

  @Test(dataProvider = "recordStoreDataProvider")
  public void testMutuallyExclusiveUpdate(StarTreeRecordStore recordStore) throws Exception
  {
    StarTreeRecord first = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 1000)
            .setTime(0L)
            .build();

    recordStore.update(first);

    StarTreeRecord second = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A2")
            .setDimensionValue("B", "B2")
            .setDimensionValue("C", "C2")
            .setMetricValue("M", 1000)
            .setTime(0L)
            .build();

    recordStore.update(second);

    Iterator<StarTreeRecord> itr = recordStore.iterator();
    Assert.assertEquals(itr.next(), first);
    Assert.assertEquals(itr.next(), second);
    Assert.assertFalse(itr.hasNext());
  }
}
