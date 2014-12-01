package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class TestStarTreeRecordStoreCircularBufferImpl
{
  private final List<String> dimensionNames = Arrays.asList("A", "B", "C");
  private final List<String> metricNames = Arrays.asList("M");
  private final Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
  private final int numTimeBuckets = 4;
  private final int numRecords = 100;

  private File rootDir;
  private UUID nodeId;
  private StarTreeRecordStoreFactory recordStoreFactory;
  private StarTreeRecordStore recordStore;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestStarTreeRecordStoreCircularBufferImpl.class.getSimpleName());

    nodeId = UUID.randomUUID();

    if (rootDir.exists())
    {
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

    new ObjectMapper().writeValue(
            new File(rootDir, nodeId + StarTreeRecordStoreFactoryCircularBufferImpl.INDEX_SUFFIX), forwardIndex);

    Properties config = new Properties();
    config.setProperty("rootDir", rootDir.getAbsolutePath());
    config.setProperty("numTimeBuckets", Integer.toString(numTimeBuckets));
    recordStoreFactory = new StarTreeRecordStoreFactoryCircularBufferImpl();
    recordStoreFactory.init(dimensionNames, metricNames, config);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    recordStore = recordStoreFactory.createRecordStore(nodeId);

    // Generate records
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    for (int i = 0; i < numRecords; i++)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A" + (i % 4))
              .setDimensionValue("B", "B" + (i % 3))
              .setDimensionValue("C", "C" + (i % 2))
              .setMetricValue("M", 1)
              .setTime((long) (i / (numRecords / numTimeBuckets)));
      records.add(builder.build());
    }

    // Add all-other record
    records.add(new StarTreeRecordImpl.Builder()
                        .setDimensionValue("A", StarTreeConstants.OTHER)
                        .setDimensionValue("B", StarTreeConstants.OTHER)
                        .setDimensionValue("C", StarTreeConstants.OTHER)
                        .setMetricValue("M", 0)
                        .setTime(0L)
                        .build());

    // Fill a buffer and write to bufferFile
    OutputStream outputStream = new FileOutputStream(
            new File(rootDir, nodeId + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX));
    StarTreeRecordStoreCircularBufferImpl.fillBuffer(
            outputStream,
            dimensionNames,
            metricNames,
            forwardIndex,
            records,
            numTimeBuckets,
            true);
    outputStream.flush();
    outputStream.close();

    // Open
    recordStore.open();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    FileUtils.forceDelete(rootDir);
  }

  @Test
  public void testIterator() throws Exception
  {
    long sum = 0;

    for (StarTreeRecord record : recordStore)
    {
      sum += record.getMetricValues().get("M");
    }

    Assert.assertEquals(sum, numRecords);
  }

  @Test
  public void testClear() throws Exception
  {
    recordStore.clear();

    long sum = 0;

    for (StarTreeRecord record : recordStore)
    {
      sum += record.getMetricValues().get("M");
    }

    Assert.assertEquals(sum, 0);
  }

  @Test
  public void testGetMetricSums() throws Exception
  {
    // Specific
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .build();
    int[] result = recordStore.getMetricSums(query);
    Assert.assertEquals(result[0], 4 + 3 + 2);

    // Aggregate
    query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();
    result = recordStore.getMetricSums(query);
    Assert.assertEquals(result[0], numRecords);
  }

  @Test
  public void testGetTimeSeries() throws Exception
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .setTimeRange(0L, 3L)
            .build();
    List<StarTreeRecord> timeSeries = recordStore.getTimeSeries(query);
    Assert.assertEquals(timeSeries.size(), 4);

    for (StarTreeRecord record : timeSeries)
    {
      Assert.assertEquals(record.getMetricValues().get("M").intValue(), 25);
    }

    // No time range
    query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();
    try
    {
      recordStore.getTimeSeries(query);
      Assert.fail();
    }
    catch (Exception e)
    {
      // Good
    }
  }

  @Test
  public void testLeastOtherMatch() throws Exception
  {
    // This will go into all-other bucket because AX is unrecognized
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "AX")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setMetricValue("M", 1)
            .setTime(0L)
            .build();

    recordStore.update(record);

    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", StarTreeConstants.OTHER)
            .setDimensionValue("B", StarTreeConstants.OTHER)
            .setDimensionValue("C", StarTreeConstants.OTHER)
            .build();

    int[] sums = recordStore.getMetricSums(query);
    Assert.assertEquals(sums[0], 1);
  }
}
