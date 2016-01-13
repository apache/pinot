package com.linkedin.thirdeye.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;

public class DimensionKeyTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionKeyTest.class);

  @Test
  public void serDeserTest() throws Exception {
    String[] dimensionValues = new String[] {
        "us", "chrome", "gmail.com", "android"
    };
    DimensionKey key = new DimensionKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
    byte[] serializedBytes;

    serializedBytes = key.toBytes();

    DimensionKey readKey;
    readKey = DimensionKey.fromBytes(serializedBytes);
    Assert.assertEquals(key, readKey);
    Assert.assertTrue(key.compareTo(readKey) == 0);
    Assert.assertTrue(key.equals(readKey));
    Assert.assertEquals(key.toMD5(), readKey.toMD5());

  }
}
