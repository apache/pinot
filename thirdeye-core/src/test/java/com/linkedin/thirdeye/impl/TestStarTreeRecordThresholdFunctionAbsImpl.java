package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            .setMetricValue("M", 1234)
            .setMetricType("M", "INT")
            .build();

    Properties props = new Properties();
    props.put("M", "1000");
    thresholdFunction.init(props);

    Map<String, List<StarTreeRecord>> sample = new HashMap<String, List<StarTreeRecord>>();
    sample.put("A1", Arrays.asList(record));

    Assert.assertEquals(thresholdFunction.apply(sample).size(), 1);
  }

  @Test
  public void testSingleRecordFail() throws Exception
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A1")
            .setDimensionValue("B", "B1")
            .setDimensionValue("C", "C1")
            .setMetricValue("M", 1234)
            .setMetricType("M", "INT")
            .build();

    Properties props = new Properties();
    props.put("M", "2000");
    thresholdFunction.init(props);

    Map<String, List<StarTreeRecord>> sample = new HashMap<String, List<StarTreeRecord>>();
    sample.put("A1", Arrays.asList(record));

    Assert.assertEquals(thresholdFunction.apply(sample).size(), 0);
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
              .setMetricValue("M", 100 * i)
              .setMetricType("M", "INT")
              .build();
      records.add(record);
    }

    Properties props = new Properties();
    props.put("M", "100");
    thresholdFunction.init(props);

    Map<String, List<StarTreeRecord>> sample = new HashMap<String, List<StarTreeRecord>>();
    sample.put("A1", records);

    Assert.assertEquals(thresholdFunction.apply(sample).size(), 1);
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
              .setMetricValue("M", 1)
              .setMetricType("M", "INT")
              .build();
      records.add(record);
    }

    Properties props = new Properties();
    props.put("M", "500");
    thresholdFunction.init(props);

    Map<String, List<StarTreeRecord>> sample = new HashMap<String, List<StarTreeRecord>>();
    sample.put("A1", records);

    Assert.assertEquals(thresholdFunction.apply(sample).size(), 0);
  }
}
