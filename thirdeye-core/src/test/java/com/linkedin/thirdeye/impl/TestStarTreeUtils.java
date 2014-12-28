package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestStarTreeUtils
{
  @Test
  public void testMergeEmptyList() throws Exception
  {
    try
    {
      StarTreeUtils.merge(new ArrayList<StarTreeRecord>());
      Assert.fail();
    }
    catch (Exception e)
    {
      // Good
    }
  }

  @Test
  public void testMergeUnequalTimes() throws Exception
  {
    try
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A1")
              .setDimensionValue("B", "B1")
              .setDimensionValue("C", "C1")
              .setMetricValue("M", 100)
              .setMetricType("M","INT");

      StarTreeRecord r1 = builder.setTime(100L).build();
      StarTreeRecord r2 = builder.setTime(200L).build();

      StarTreeUtils.merge(Arrays.asList(r1, r2));
    }
    catch (Exception e)
    {
      // Good
    }
  }

  @Test
  public void testMerge() throws Exception
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 100)
            .setMetricType("M","INT")
            .setTime(100L);

    StarTreeRecord r1 = builder.build();
    StarTreeRecord r2 = builder.build();
    StarTreeRecord r3 = StarTreeUtils.merge(Arrays.asList(r1, r2));

    Assert.assertEquals(r3.getDimensionValues().get("A"), "A1");
    Assert.assertEquals(r3.getDimensionValues().get("B"), "B1");
    Assert.assertEquals(r3.getDimensionValues().get("C"), "C1");
    Assert.assertEquals(r3.getMetricValues().get("M").intValue(), 200);
  }

  @Test
  public void testFilterQueries() throws Exception
  {
    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection("dummy")
            .setMaxRecordStoreEntries(4)
            .setMetricNames(Arrays.asList("M"))
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .setMetricTypes(Arrays.asList("INT"))
            .build();

    StarTree starTree = new StarTreeImpl(config);
    starTree.open();

    for (int i = 0; i < 100; i++)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionValue("A", "A" + (i % 2));
      builder.setDimensionValue("B", "B" + (i % 4));
      builder.setDimensionValue("C", "C" + (i % 8));
      builder.setMetricValue("M", 1);
      builder.setMetricType("M","INT");
      builder.setTime((long) i);
      starTree.add(builder.build());
    }

    StarTreeQuery baseQuery = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", StarTreeConstants.ALL)
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();

    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);
    Assert.assertEquals(queries.size(), 2); // A0 and A1

    Map<String, List<String>> filter = new HashMap<String, List<String>>();
    filter.put("A", Arrays.asList("A0"));

    List<StarTreeQuery> filteredQueries = StarTreeUtils.filterQueries(queries, filter);
    Assert.assertEquals(filteredQueries.size(), 1);
    Assert.assertEquals(filteredQueries.get(0).getDimensionValues().get("A"), "A0");

    starTree.close();
  }

  @Test
  public void testAvroConversion() throws Exception
  {
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("MyRecord.avsc"));

    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection("MyRecord")
            .setMetricNames(Arrays.asList("M"))
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .setMetricTypes(Arrays.asList("INT"))
            .setTimeColumnName("hoursSinceEpoch")
            .build();

    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setMetricValue("M", 100)
            .setMetricType("M", "INT")
            .setTime(100L)
            .build();

    GenericRecord genericRecord = StarTreeUtils.toGenericRecord(config, schema, record, null);

    Assert.assertEquals(genericRecord.get("A"), "A0");
    Assert.assertEquals(genericRecord.get("B"), "B0");
    Assert.assertEquals(genericRecord.get("C"), "C0");
    Assert.assertEquals(((Number) genericRecord.get("M")).longValue(), 100L);

    StarTreeRecord convertedRecord = StarTreeUtils.toStarTreeRecord(config, genericRecord);

    Assert.assertEquals(convertedRecord, record);
  }
}
