package com.linkedin.thirdeye.rootcause.impl;

import org.testng.Assert;
import org.testng.annotations.Test;


public class EntityUtilsTest {
  private static final String URN_TEST_VECTOR_DECODED = " A BCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()-_=+`~[{]}\\|\'\";:/?,<. > ";
  private static final String URN_TEST_VECTOR_ENCODED = "%20A%20BCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!%40%23%24%25%5E%26*()-_%3D%2B%60~%5B%7B%5D%7D%5C%7C'%22%3B%3A%2F%3F%2C%3C.%20%3E%20";

  @Test
  public void testEncodeURN() {
    Assert.assertEquals(EntityUtils.encodeURNComponent(URN_TEST_VECTOR_DECODED), URN_TEST_VECTOR_ENCODED);
  }

  @Test
  public void testDecodeURN() {
    Assert.assertEquals(EntityUtils.decodeURNComponent(URN_TEST_VECTOR_ENCODED), URN_TEST_VECTOR_DECODED);
  }
}
