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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    int entrySize = StarTreeRecordStoreFixedBufferImpl.getEntrySize(dimensionNames, metricNames);
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, numEntries * entrySize);

    for (int i = 0; i < numEntries; i++)
    {
      StarTreeRecord record = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A" + i % 250)
              .setDimensionValue("B", "B" + i % 500)
              .setDimensionValue("C", "C" + i)
              .setMetricValue("M", 1L)
              .setTime((long) i / 250)
              .build();

      StarTreeRecordStoreFixedBufferImpl.writeRecord(buffer, record, dimensionNames, metricNames, forwardIndex, 4);
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

  @Test
  public void testGetDimensions() throws Exception
  {
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(), "C");
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(Arrays.asList("C")), "B");
    Assert.assertEquals(recordStore.getCardinality("C"), 1000);

    Set<String> expectedValues = new HashSet<String>();
    for (int i = 0; i < 250; i++)
    {
      expectedValues.add("A" + i);
    }
    Assert.assertEquals(recordStore.getDimensionValues("A"), expectedValues);
  }

  @Test
  public void testRollOver() throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setTime(4L) // to 0 bucket
            .setMetricValue("M", 100L)
            .build();

    recordStore.update(record);

    // Time 0 should have nothing in it now
    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(0L)))
            .build();
    long[] metricSums = recordStore.getMetricSums(starTreeQuery);
    long[] expected = new long[] { 0 };
    Assert.assertEquals(metricSums, expected);

    // Time 4 should have 100 (without the 1 from previous bucket)
    starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(4L)))
            .build();
    metricSums = recordStore.getMetricSums(starTreeQuery);
    expected = new long[] { 100 };
    Assert.assertEquals(metricSums, expected);
  }
}
