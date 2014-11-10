package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestStarTreeImpl
{
  private File rootDir;
  private StarTreeConfig config;
  private StarTreeRecordStoreFactory recordStoreFactory;
  private StarTree starTree;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestStarTreeImpl.class.getName());
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
    try { FileUtils.forceMkdir(rootDir); } catch (Exception e) { /* ok */ }

    config = new StarTreeConfig.Builder()
            .setMaxRecordStoreEntries(4)
            .setMetricNames(Arrays.asList("M"))
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .build();

    starTree = new StarTreeImpl(config);
    starTree.open();

    for (int i = 0; i < 100; i++)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionValue("A", "A" + (i % 2));
      builder.setDimensionValue("B", "B" + (i % 4));
      builder.setDimensionValue("C", "C" + (i % 8));
      builder.setMetricValue("M", 1L);
      builder.setTime((long) i);
      starTree.add(builder.build());
    }

    // A specific extra record
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
    builder.setDimensionValue("A", "AX");
    builder.setDimensionValue("B", "BX");
    builder.setDimensionValue("C", "CX");
    builder.setMetricValue("M", 1L);
    starTree.add(builder.build());
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    starTree.close();
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
  }

  @Test
  public void testQuery() throws Exception
  {
    // All
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setDimensionValue("A", "*");
    queryBuilder.setDimensionValue("B", "*");
    queryBuilder.setDimensionValue("C", "*");
    StarTreeRecord result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 100L + 1); // the extra

    // Half
    queryBuilder.setDimensionValue("A", "A0");
    queryBuilder.setDimensionValue("B", "*");
    queryBuilder.setDimensionValue("C", "*");
    result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 50L);

    // Quarter
    queryBuilder.setDimensionValue("A", "*");
    queryBuilder.setDimensionValue("B", "B0");
    queryBuilder.setDimensionValue("C", "*");
    result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 25L);

    // Specific
    queryBuilder.setDimensionValue("A", "AX");
    queryBuilder.setDimensionValue("B", "BX");
    queryBuilder.setDimensionValue("C", "CX");
    result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 1L);
  }

  @Test
  public void testTimeRangeQuery() throws Exception
  {
    // All
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setDimensionValue("A", "*");
    queryBuilder.setDimensionValue("B", "*");
    queryBuilder.setDimensionValue("C", "*");
    queryBuilder.setTimeRange(0L, 50L);
    StarTreeRecord result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 50L + 1); // the extra
  }

  @Test
  public void testTimeBucketsQuery() throws Exception
  {
    // Odd buckets (#=> 50)
    Set<Long> buckets = new HashSet<Long>();
    for (long i = 0; i < 100; i++)
    {
      if (i % 2 == 0)
      {
        buckets.add(i);
      }
    }

    // All
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setDimensionValue("A", "*");
    queryBuilder.setDimensionValue("B", "*");
    queryBuilder.setDimensionValue("C", "*");
    queryBuilder.setTimeBuckets(buckets);
    StarTreeRecord result = starTree.query(queryBuilder.build());
    Assert.assertEquals(result.getMetricValues().get("M").longValue(), 50L); // the extra time was null
  }

  @Test
  public void testTimeQueryMutualExclusion() throws Exception
  {
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setDimensionValue("A", "*");
    queryBuilder.setDimensionValue("B", "*");
    queryBuilder.setDimensionValue("C", "*");
    queryBuilder.setTimeBuckets(new HashSet<Long>());
    queryBuilder.setTimeRange(0L, 10L);

    try
    {
      queryBuilder.build();
      Assert.fail();
    }
    catch (Exception e)
    {
      // Good
    }
  }

  @Test
  public void testGetDimensionValues() throws Exception
  {
    Set<String> aValues = starTree.getDimensionValues("A");
    Set<String> expectedValues = new HashSet<String>(Arrays.asList("A0", "A1", "AX"));
    Assert.assertEquals(aValues, expectedValues);
  }

  @Test
  public void testGetOtherDimensionValues() throws Exception
  {
    // TODO: Need a more complex tree with rollup, so this is just smoke test for now
    Set<String> otherValues = starTree.getOtherDimensionValues("A");
    Assert.assertEquals(otherValues, new HashSet<String>());
  }

  @Test
  public void testSerialization() throws Exception
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
    objectOutputStream.writeObject(starTree.getRoot());
    objectOutputStream.flush();

    // TODO: Read it back
  }
}
