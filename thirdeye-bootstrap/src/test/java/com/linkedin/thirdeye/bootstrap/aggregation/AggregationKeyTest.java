package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class AggregationKeyTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(AggregationKeyTest.class);
  
  @Test
  public void serDeserTest() {
    List<String> dimensionValues = Lists.newArrayList("", "chrome",
        "gmail.com", "android");
    AggregationKey key = new AggregationKey(dimensionValues);
    System.out.println("tostring--" + key.toString());

    DataOutputBuffer out = new DataOutputBuffer();
    try {
      key.write(out);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    byte[] data = out.getData();
    AggregationKey readKey = new AggregationKey();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(data, data.length);
    try {
      readKey.readFields(in);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Assert.assertEquals(key, readKey);
  }
}
