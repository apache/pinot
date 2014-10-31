package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestStarTreeRecordThresholdFunctionAbsImpl
{
  private StarTreeRecordThresholdFunction thresholdFunction;

  @BeforeMethod
  public void beforeMethod()
  {
    thresholdFunction = new StarTreeRecordThresholdFunctionAbsImpl();
  }

  @Test
  public void testSingleRecordPass() throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 1234L)
            .build();

    Properties props = new Properties();
    props.put("M", "1000");
    thresholdFunction.init(props);

    Assert.assertTrue(thresholdFunction.passesThreshold(Arrays.asList(record)));
  }

  @Test
  public void testSingleRecordFail() throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 1234L)
            .build();

    Properties props = new Properties();
    props.put("M", "2000");
    thresholdFunction.init(props);

    Assert.assertFalse(thresholdFunction.passesThreshold(Arrays.asList(record)));
  }

  @Test
  public void testMultipleRecordsPass() throws Exception
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    for (int i = 1; i <= 10; i++)
    {
      StarTreeRecord record = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A1")
              .setDimensionValue("B", "B1")
              .setDimensionValue("C", "C1")
              .setMetricValue("M", 100L * i)
              .build();
      records.add(record);
    }

    Properties props = new Properties();
    props.put("M", "100");
    thresholdFunction.init(props);

    Assert.assertTrue(thresholdFunction.passesThreshold(records));
  }

  @Test
  public void testMultipleRecordsFail() throws Exception
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    for (int i = 1; i <= 10; i++)
    {
      StarTreeRecord record = new StarTreeRecordImpl.Builder()
              .setDimensionValue("A", "A1")
              .setDimensionValue("B", "B1")
              .setDimensionValue("C", "C1")
              .setMetricValue("M", 1L)
              .build();
      records.add(record);
    }

    Properties props = new Properties();
    props.put("M", "500");
    thresholdFunction.init(props);

    Assert.assertFalse(thresholdFunction.passesThreshold(records));
  }
}
