package org.apache.pinot.common.upsert.hash;

import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MD5UpsertHashFunctionTest {
  @Test
  public void testHashIsExpected() {
    assertEquals(BytesUtils.toHexString(MD5UpsertHashFunction.hashMD5("hello world".getBytes())),
        "5eb63bbbe01eeed093cb22bb8f5acdc3");
  }
}
