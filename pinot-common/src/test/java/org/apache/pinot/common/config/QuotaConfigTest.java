/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.config;

import java.io.IOException;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QuotaConfigTest {

  @Test
  public void testQuotaConfig()
      throws IOException {
    {
      String quotaConfigStr = "{\"storage\" : \"100g\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);

      Assert.assertEquals(quotaConfig.getStorage(), "100g");
      Assert.assertEquals(quotaConfig.storageSizeBytes(), 100 * 1024 * 1024 * 1024L);
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      Assert.assertNull(quotaConfig.getStorage());
      Assert.assertEquals(quotaConfig.storageSizeBytes(), -1);
    }
  }

  @Test
  public void testBadQuotaConfig()
      throws IOException {
    {
      String quotaConfigStr = "{\"storage\" : \"124GB3GB\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      Assert.assertNotNull(quotaConfig.getStorage());
      Assert.assertEquals(quotaConfig.storageSizeBytes(), -1);
    }
  }

  @Test(expectedExceptions = ConfigurationRuntimeException.class)
  public void testBadConfig()
      throws IOException {
    String quotaConfigStr = "{\"storage\":\"-1M\"}";
    QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
    quotaConfig.validate();
  }

  @Test
  public void testQpsQuota()
      throws IOException {
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"100.00\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);

      Assert.assertNotNull(quotaConfig.getMaxQueriesPerSecond());
      Assert.assertEquals(quotaConfig.getMaxQueriesPerSecond(), "100.00");
      Assert.assertTrue(quotaConfig.isMaxQueriesPerSecondValid());
    }
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"0.5\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);

      Assert.assertNotNull(quotaConfig.getMaxQueriesPerSecond());
      Assert.assertEquals(quotaConfig.getMaxQueriesPerSecond(), "0.5");
      Assert.assertTrue(quotaConfig.isMaxQueriesPerSecondValid());
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      Assert.assertNull(quotaConfig.getMaxQueriesPerSecond());
      Assert.assertTrue(quotaConfig.isMaxQueriesPerSecondValid());
    }
  }

  @Test(expectedExceptions = ConfigurationRuntimeException.class)
  public void testInvalidQpsQuota()
      throws IOException {
    String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"InvalidQpsQuota\"}";
    QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
    Assert.assertNotNull(quotaConfig.getMaxQueriesPerSecond());
    quotaConfig.validate();
  }

  @Test(expectedExceptions = ConfigurationRuntimeException.class)
  public void testNegativeQpsQuota()
      throws IOException {
    String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"-1.0\"}";
    QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
    Assert.assertNotNull(quotaConfig.getMaxQueriesPerSecond());
    quotaConfig.validate();
  }

  @Test(expectedExceptions = ConfigurationRuntimeException.class)
  public void testBadQpsQuota()
      throws IOException {
    String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"1.0Test\"}";
    QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
    Assert.assertNotNull(quotaConfig.getMaxQueriesPerSecond());
    quotaConfig.validate();
  }
}
