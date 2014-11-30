package com.linkedin.thirdeye.bootstrap.aggregation;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class AggregationKeyTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(AggregationKeyTest.class);

  @Test
  public void serDeserTest() throws Exception {
    String[] dimensionValues = new String[] { "us", "chrome", "gmail.com",
        "android" };
    AggregationKey key = new AggregationKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
    byte[] serializedBytes;

    serializedBytes = key.toBytes();

    AggregationKey readKey;
    readKey = AggregationKey.fromBytes(serializedBytes);
    Assert.assertEquals(key, readKey);
  }
}
