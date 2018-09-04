package com.linkedin.thirdeye.anomaly.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestEmailUtils {
  @Test
  public void testIsValidEmailAddress() {
    Assert.assertTrue(EmailUtils.isValidEmailAddress("user@host.domain"));

    Assert.assertFalse(EmailUtils.isValidEmailAddress(null));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("    "));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("user"));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("@host"));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("u ser@host.domain"));
  }

  @Test
  public void testGetValidEmailAddresses() {
    String emailAddresses = "user1@host1.domain1,user2@host1.domain1";
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses), emailAddresses);
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses + ",user"), emailAddresses);
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses + ",user1@host1.domain1"), emailAddresses);
    Assert.assertEquals(EmailUtils.getValidEmailAddresses(emailAddresses + ",, , user1@host1.domain1,  "), emailAddresses);
  }
}
