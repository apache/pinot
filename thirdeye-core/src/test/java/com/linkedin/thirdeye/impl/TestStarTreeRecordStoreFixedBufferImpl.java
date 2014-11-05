package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStarTreeRecordStoreFixedBufferImpl
{
  private final Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
  private final List<String> dimensionNames = Arrays.asList("A", "B", "C");
  private final List<String> metricNames = Arrays.asList("M");

  private File file;
  private StarTreeRecordStore recordStore;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    file = new File(System.getProperty("java.io.tmpdir")
                            + File.separator
                            + TestStarTreeRecordStoreFixedBufferImpl.class.getSimpleName() + ".buffer");

    // Pick dimension values
    int valueId = 1;
    for (String dimensionName : dimensionNames)
    {
      Map<String, Integer> valueIds = new HashMap<String, Integer>();
      for (int i = 0; i < 1000; i++)
      {
        valueIds.put(dimensionName + i, valueId++);
      }
      forwardIndex.put(dimensionName, valueIds);
    }

    // Load some data into file
    int numEntries = 1000;
    int entrySize = dimensionNames.size() * Integer.SIZE / 8 + (metricNames.size() + 1) * Long.SIZE / 8;
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, numEntries * entrySize);
    for (int i = 0; i < numEntries; i++)
    {
      long time = i / 250;  // 4 buckets
      String aVal = "A" + i % 250;
      String bVal = "B" + i % 500;
      String cVal = "C" + i;
      long mVal = 1;  // all same

      buffer.putLong(time);
      buffer.putInt(forwardIndex.get("A").get(aVal));
      buffer.putInt(forwardIndex.get("B").get(bVal));
      buffer.putInt(forwardIndex.get("C").get(cVal));
      buffer.putLong(mVal);
    }
    buffer.force();

    recordStore = new StarTreeRecordStoreFixedBufferImpl(file, dimensionNames, metricNames, forwardIndex);
    recordStore.open();
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    recordStore.close();
    FileUtils.forceDelete(file);
  }

  @Test
  public void testIterate() throws Exception
  {
    long sum = 0;
    for (StarTreeRecord record : recordStore)
    {
      sum += record.getMetricValues().get("M");
    }
    Assert.assertEquals(sum, 1000);
  }

  @Test
  public void testGetMetricSums() throws Exception
  {
    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .build();

    long[] metricSums = recordStore.getMetricSums(starTreeQuery);
    long[] expected = new long[] { 1 };

    Assert.assertEquals(metricSums, expected);
  }

  @Test
  public void testUpdate() throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setTime(0L)
            .setMetricValue("M", 100L)
            .build();

    recordStore.update(record);

    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .build();

    long[] metricSums = recordStore.getMetricSums(starTreeQuery);
    long[] expected = new long[] { 101 };

    Assert.assertEquals(metricSums, expected);
  }
}
