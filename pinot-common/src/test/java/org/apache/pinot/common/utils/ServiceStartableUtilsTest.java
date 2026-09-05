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

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class ServiceStartableUtilsTest {
  private static final String GROOVY_POLICY_KEY = CommonConstants.Groovy.DISABLE_INGESTION_GROOVY;

  @Test
  public void testMissingClusterPolicyPreservesExplicitOptIn() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(GROOVY_POLICY_KEY, "false");

    ServiceStartableUtils.applyIngestionGroovyPolicy(config, null, ServiceRole.SERVER);

    assertEquals(config.getProperty(GROOVY_POLICY_KEY), "false");
  }

  @Test
  public void testMissingOrInvalidPolicyFailsClosed() {
    PinotConfiguration missingConfig = new PinotConfiguration();
    ServiceStartableUtils.applyIngestionGroovyPolicy(missingConfig, null, ServiceRole.MINION);
    assertEquals(missingConfig.getProperty(GROOVY_POLICY_KEY), "true");

    PinotConfiguration invalidConfig = new PinotConfiguration();
    invalidConfig.setProperty(GROOVY_POLICY_KEY, "invalid");
    ServiceStartableUtils.applyIngestionGroovyPolicy(invalidConfig, null, ServiceRole.SERVER);
    assertEquals(invalidConfig.getProperty(GROOVY_POLICY_KEY), "true");
  }

  @Test
  public void testClusterPolicyIsAuthoritative() {
    PinotConfiguration config = new PinotConfiguration();
    ServiceStartableUtils.applyIngestionGroovyPolicy(config, "false", ServiceRole.SERVER);
    assertEquals(config.getProperty(GROOVY_POLICY_KEY), "false");

    PinotConfiguration conflictingConfig = new PinotConfiguration();
    conflictingConfig.setProperty(GROOVY_POLICY_KEY, "false");
    IllegalStateException error = expectThrows(IllegalStateException.class,
        () -> ServiceStartableUtils.applyIngestionGroovyPolicy(conflictingConfig, "true", ServiceRole.MINION));
    assertEquals(error.getMessage(), String.format(
        "Conflicting ingestion Groovy policy: cluster config '%s=true' is authoritative, but the minion instance "
            + "config resolves to false", GROOVY_POLICY_KEY));
  }

  @Test
  public void testResolvedPolicyCannotBeOverridden() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(GROOVY_POLICY_KEY, "false");

    expectThrows(IllegalStateException.class,
        () -> ServiceStartableUtils.enforceIngestionGroovyPolicy(config, true, ServiceRole.CONTROLLER));
  }
}
