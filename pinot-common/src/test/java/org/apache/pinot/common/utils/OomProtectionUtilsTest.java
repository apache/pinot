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
package org.apache.pinot.common.utils;

import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OomProtectionUtilsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(OomProtectionUtilsTest.class);

  @Test
  public void testHasDisableAdaptiveIhopFlag() {
    Assert.assertFalse(OomProtectionUtils.hasDisableAdaptiveIhopFlag(null));
    Assert.assertFalse(OomProtectionUtils.hasDisableAdaptiveIhopFlag(Collections.emptyList()));
    Assert.assertFalse(OomProtectionUtils.hasDisableAdaptiveIhopFlag(Arrays.asList("-Xmx1G", "-Xms1G")));
    Assert.assertTrue(OomProtectionUtils.hasDisableAdaptiveIhopFlag(
        Arrays.asList("-Xmx1G", "-XX:-G1UseAdaptiveIHOP", "-Xms1G")));
  }

  @Test
  public void testEnforceIhopGcOrDisableOom() {
    PinotConfiguration cfg = new PinotConfiguration();
    cfg.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);

    boolean changed = OomProtectionUtils.enforceIhopGcOrDisableOom(cfg,
        Arrays.asList("-Xmx1G", "-Xms1G"));
    Assert.assertTrue(changed);
    Assert.assertFalse(cfg.getProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true));

    PinotConfiguration cfg2 = new PinotConfiguration();
    cfg2.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    boolean changed2 = OomProtectionUtils.enforceIhopGcOrDisableOom(cfg2,
        Arrays.asList("-XX:-G1UseAdaptiveIHOP", "-Xmx1G"));
    Assert.assertFalse(changed2);
    Assert.assertTrue(cfg2.getProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, false));
  }
}
