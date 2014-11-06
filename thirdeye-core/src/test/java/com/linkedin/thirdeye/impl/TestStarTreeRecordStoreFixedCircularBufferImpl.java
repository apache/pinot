package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class TestStarTreeRecordStoreFixedCircularBufferImpl
{
  private final Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
  private final List<String> dimensionNames = Arrays.asList("A", "B", "C");
  private final List<String> metricNames = Arrays.asList("M");

  private File rootDir;
  private File bufferFile;
  private StarTreeRecordStore recordStore;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"),
                    TestStarTreeRecordStoreFixedCircularBufferImpl.class.getSimpleName());
    FileUtils.forceMkdir(rootDir);
    UUID nodeId = UUID.randomUUID();
    bufferFile = new File(rootDir, nodeId.toString() + StarTreeRecordStoreFactoryFixedCircularBufferImpl.BUFFER_SUFFIX);

    // Pick dimension values
    int valueId = StarTreeConstants.FIRST_VALUE;
    for (String dimensionName : dimensionNames)
    {
      Map<String, Integer> valueIds = new HashMap<String, Integer>();
      for (int i = 0; i < 1000; i++)
      {
        valueIds.put(dimensionName + i, valueId++);
      }
      valueIds.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      valueIds.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
      forwardIndex.put(dimensionName, valueIds);
    }

    // Load some data into file
    int numEntries = 1000;
    int entrySize = StarTreeRecordStoreFixedCircularBufferImpl.getEntrySize(dimensionNames, metricNames);
    FileChannel fileChannel = new RandomAccessFile(bufferFile, "rw").getChannel();
    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (numEntries + 4) * entrySize);

    // Write leading "other" record
    StarTreeRecord otherRecord = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", StarTreeConstants.OTHER)
            .setDimensionValue("B", StarTreeConstants.OTHER)
            .setDimensionValue("C", StarTreeConstants.OTHER)
            .setMetricValue("M", 0L)
            .setTime(0L)
            .build();
    StarTreeRecordStoreFixedCircularBufferImpl.writeRecord(buffer, otherRecord, dimensionNames, metricNames, forwardIndex, 4);

    long lastTime = 0;
    for (int i = 0; i < numEntries; i++)
    {
      long currentTime = (long) i / 250;

      // Write leading "other" record on roll over
      if (lastTime < currentTime)
      {
        otherRecord = new StarTreeRecordImpl.Builder()
                .setDimensionValue("A", StarTreeConstants.OTHER)
                .setDimensionValue("B", StarTreeConstants.OTHER)
                .setDimensionValue("C", StarTreeConstants.OTHER)
                .setMetricValue("M", 0L)
                .setTime(currentTime)
                .build();
        StarTreeRecordStoreFixedCircularBufferImpl.writeRecord(buffer, otherRecord, dimensionNames, metricNames, forwardIndex, 4);
      }

      StarTreeRecord record = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A" + i % 250)
              .setDimensionValue("B", "B" + i % 500)
              .setDimensionValue("C", "C" + i)
              .setMetricValue("M", 1L)
              .setTime(currentTime)
              .build();

      StarTreeRecordStoreFixedCircularBufferImpl.writeRecord(buffer, record, dimensionNames, metricNames, forwardIndex, 4);

      lastTime = currentTime;
    }
    buffer.force();

    // Write forward index to file
    File forwardIndexFile = new File(rootDir, nodeId.toString() + StarTreeRecordStoreFactoryFixedCircularBufferImpl.INDEX_SUFFIX);
    OutputStream outputStream = new FileOutputStream(forwardIndexFile);
    new ObjectMapper().writeValue(outputStream, forwardIndex);
    outputStream.flush();
    outputStream.close();

    // Use factory to construct record store
    Properties props = new Properties();
    props.put("rootDir", rootDir.getAbsolutePath());
    StarTreeRecordStoreFactory recordStoreFactory = new StarTreeRecordStoreFactoryFixedCircularBufferImpl();
    recordStoreFactory.init(dimensionNames, metricNames, props);
    recordStore = recordStoreFactory.createRecordStore(nodeId);
    recordStore.open();
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    recordStore.close();
    FileUtils.forceDelete(rootDir);
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
    // Specific record
    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .build();

    long[] metricSums = recordStore.getMetricSums(starTreeQuery);

    Assert.assertEquals(metricSums[0], 1L);

    // All records
    starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();

    metricSums = recordStore.getMetricSums(starTreeQuery);

    Assert.assertEquals(metricSums[0], 1000L);
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

    Assert.assertEquals(metricSums[0], 101L);
  }

  @Test
  public void testGetDimensions() throws Exception
  {
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(), "C");
    Assert.assertEquals(recordStore.getMaxCardinalityDimension(Arrays.asList("C")), "B");
    Assert.assertEquals(recordStore.getCardinality("C"), 1000 + 1); // the "other"

    Set<String> expectedValues = new HashSet<String>();
    for (int i = 0; i < 250; i++)
    {
      expectedValues.add("A" + i);
    }
    expectedValues.add("?");
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
    Assert.assertEquals(metricSums[0], 0L);

    // Time 4 should have 100 (without the 1 from previous bucket)
    starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(4L)))
            .build();
    metricSums = recordStore.getMetricSums(starTreeQuery);
    Assert.assertEquals(metricSums[0], 100L);
  }

  @Test
  public void testUpdateAllUnknown() throws Exception
  {
    // No bucket for this, would expect to go to ?,?,?
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "AX")
            .setDimensionValue("B", "BX")
            .setDimensionValue("C", "CX")
            .setTime(0L)
            .setMetricValue("M", 100L)
            .build();

    recordStore.update(record);

    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "?")
            .setDimensionValue("B", "?")
            .setDimensionValue("C", "?")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(0L)))
            .build();
    long[] metricSums = recordStore.getMetricSums(starTreeQuery);
    Assert.assertEquals(metricSums[0], 100L);
  }

  @Test
  public void testUpdatePartialUnknown() throws Exception
  {
    // No bucket for this, would expect to go to ?,?,?
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "BX")
            .setDimensionValue("C", "CX")
            .setTime(0L)
            .setMetricValue("M", 100L)
            .build();

    recordStore.update(record);

    StarTreeQuery starTreeQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "?")
            .setDimensionValue("B", "?")
            .setDimensionValue("C", "?")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(0L)))
            .build();
    long[] metricSums = recordStore.getMetricSums(starTreeQuery);
    Assert.assertEquals(metricSums[0], 100L);
  }
}
