package com.linkedin.thirdeye.anomaly.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestEmailUtils {
  @Test
  public void testIsValidEmailAddress() {
    Assert.assertTrue(EmailUtils.isValidEmailAddress("abc@cba.abc"));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("abc"));
  }


  @Test
  public void testGetValidEmailAddresses() {
    String emailAddresses = "abc@cba.abc,bcd@dcb.bcd";
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses), emailAddresses);
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses + ",abc"), emailAddresses);
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses + ",abc@cba.abc"), emailAddresses);
  }
}
