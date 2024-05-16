package org.apache.pinot.common.upsert.hash;

import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class Murmur3UpsertHashFunctionTest {
  @Test
  public void testHashIsExpected() {
    assertEquals(BytesUtils.toHexString(Murmur3UpsertHashFunction.hashMurmur3("hello world".getBytes())),
        "0e617feb46603f53b163eb607d4697ab");
  }
}