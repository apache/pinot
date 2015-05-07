package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TestStarTreeImpl
{
  private File rootDir;
  private StarTreeConfig config;
  private StarTreeRecordStoreFactory recordStoreFactory;
  private StarTree starTree;
  private MetricSchema metricSchema;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestStarTreeImpl.class.getName());
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
    try { FileUtils.forceMkdir(rootDir); } catch (Exception e) { /* ok */ }

    config = new StarTreeConfig.Builder()
            .setCollection("dummy")
            .setSplit(new SplitSpec(4, null))
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
            .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")))
            .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
            .setFixed(false)
            .build();

    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    starTree = new StarTreeImpl(config);
    starTree.open();

    for (int i = 0; i < 100; i++)
    {
      MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
      ts.set(i, "M", 1);

      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionKey(getDimensionKey("A" + (i % 2), "B" + (i % 4), "C" + (i % 8)));
      builder.setMetricTimeSeries(ts);
      starTree.add(builder.build(config));
    }

    MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
    ts.set(0, "M", 1);

    // A specific extra record
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
    builder.setDimensionKey(getDimensionKey("AX", "BX", "CX"));
    builder.setMetricTimeSeries(ts);
    starTree.add(builder.build(config));
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
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setTimeRange(new TimeRange(0L, 100L));

    // All
    queryBuilder.setDimensionKey(getDimensionKey("*", "*", "*"));
    MetricTimeSeries result = starTree.getTimeSeries(queryBuilder.build(config));
    Assert.assertEquals(result.getMetricSums()[0].intValue(), 100 + 1); // the extra

    // Half
    queryBuilder.setDimensionKey(getDimensionKey("A0", "*", "*"));
    result = starTree.getTimeSeries(queryBuilder.build(config));
    Assert.assertEquals(result.getMetricSums()[0].intValue(), 50);

    // Quarter
    queryBuilder.setDimensionKey(getDimensionKey("*", "B0", "*"));
    result = starTree.getTimeSeries(queryBuilder.build(config));
    Assert.assertEquals(result.getMetricSums()[0].intValue(), 25);

    // Specific
    queryBuilder.setDimensionKey(getDimensionKey("AX", "BX", "CX"));
    result = starTree.getTimeSeries(queryBuilder.build(config));
    Assert.assertEquals(result.getMetricSums()[0].intValue(), 1);
  }

  @Test
  public void testTimeRangeQuery() throws Exception
  {
    // All
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    queryBuilder.setDimensionKey(getDimensionKey("*", "*", "*"));
    queryBuilder.setTimeRange(new TimeRange(0L, 49L));
    MetricTimeSeries result = starTree.getTimeSeries(queryBuilder.build(config));
    Assert.assertEquals(result.getMetricSums()[0].intValue(), 50 + 1); // the extra
  }

  @Test
  public void testGetDimensionValues() throws Exception
  {
    Set<String> aValues = starTree.getDimensionValues("A", null);
    Set<String> expectedValues = new HashSet<String>(Arrays.asList("A0", "A1", "AX", "?"));
    Assert.assertEquals(aValues, expectedValues);
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

  @Test
  public void testFindAll() throws Exception
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey("*", "*", "C0"))
            .build(config);

    Collection<StarTreeNode> nodes = starTree.findAll(query);

    Assert.assertEquals(nodes.size(), 2); // all stars, and specific C0 node
  }

  private DimensionKey getDimensionKey(String a, String b, String c)
  {
    return new DimensionKey(new String[] {a, b, c});
  }
}
