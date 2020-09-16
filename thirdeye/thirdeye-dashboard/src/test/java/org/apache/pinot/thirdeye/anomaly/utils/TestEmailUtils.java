/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.anomaly.utils;

import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestEmailUtils {
  @Test
  public void testIsValidEmailAddress() {
    Assert.assertTrue(EmailUtils.isValidEmailAddress("user@host.domain"));
    Assert.assertTrue(EmailUtils.isValidEmailAddress("user+suffix@host.domain"));
    Assert.assertTrue(EmailUtils.isValidEmailAddress("user+suffix-hyphen@host.domain"));

    Assert.assertFalse(EmailUtils.isValidEmailAddress(null));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("    "));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("user"));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("@host"));
    Assert.assertFalse(EmailUtils.isValidEmailAddress("u ser@host.domain"));
  }

  @Test
  public void testGetValidEmailAddresses() {
    String emailAddresses = "user1@host1.domain1,user2@host1.domain1";

    Set<String> emailAddressesExpected = new HashSet<>();
    emailAddressesExpected.add("user1@host1.domain1");
    emailAddressesExpected.add("user2@host1.domain1");

    Assert.assertTrue(EmailUtils.getValidEmailAddresses(emailAddresses).equals(emailAddressesExpected));
    Assert.assertTrue(EmailUtils.getValidEmailAddresses(emailAddresses + ",user").equals(emailAddressesExpected));
    Assert.assertTrue(EmailUtils.getValidEmailAddresses(emailAddresses + ",user1@host1.domain1").equals(emailAddressesExpected));
    Assert.assertTrue(EmailUtils.getValidEmailAddresses(emailAddresses + ",, , user1@host1.domain1,  ").equals(emailAddressesExpected));
  }
}
