package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

public class TestStarTreeQueryImpl
{
  @Test
  public void testGetStarDimensionNames() throws Exception
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .build();

    Assert.assertEquals(query.getStarDimensionNames(), new HashSet<String>(Arrays.asList("A", "C")));
  }

  @Test
  public void testEqualsNoTime() throws Exception
  {
    StarTreeQuery q1 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .build();

    StarTreeQuery q2 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .build();

    StarTreeQuery q3 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();

    Assert.assertEquals(q1, q2);
    Assert.assertNotEquals(q1, q3);
  }

  @Test
  public void testEqualsTimeBuckets() throws Exception
  {
    StarTreeQuery q1 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(1L, 2L, 3L)))
            .build();

    StarTreeQuery q2 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(1L, 2L, 3L)))
            .build();

    StarTreeQuery q3 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(2L, 3L))) // different buckets
            .build();

    StarTreeQuery q4 = new StarTreeQueryImpl.Builder() // no buckets
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();

    StarTreeQuery q5 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .setTimeRange(1L, 3L) // range not buckets
            .build();

    Assert.assertEquals(q2, q1);
    Assert.assertNotEquals(q3, q1);
    Assert.assertNotEquals(q4, q1);
    Assert.assertNotEquals(q5, q1);
  }

  @Test
  public void testEqualsTimeRange() throws Exception
  {
    StarTreeQuery q1 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeRange(1L, 3L)
            .build();

    StarTreeQuery q2 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeRange(1L, 3L)
            .build();

    StarTreeQuery q3 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(2L, 3L))) // different buckets
            .build();

    StarTreeQuery q4 = new StarTreeQueryImpl.Builder() // no buckets
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .build();

    StarTreeQuery q5 = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "*")
            .setTimeBuckets(new HashSet<Long>(Arrays.asList(1L, 2L, 3L))) // buckets not range
            .build();

    Assert.assertEquals(q2, q1);
    Assert.assertNotEquals(q3, q1);
    Assert.assertNotEquals(q4, q1);
    Assert.assertNotEquals(q5, q1);
  }
}
