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
package org.apache.pinot.integration.tests.custom;

import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;


/// Fails closed after a shared custom-cluster participant reports a cleanup failure.
/// Supports one sequential suite per JVM; concurrent suites in the same JVM are not supported.
/// Synchronized access protects failure reporting between callbacks.
public abstract class CustomSuiteHealthCheckedIntegrationTest extends BaseClusterIntegrationTest {
  private static Throwable _customSuiteCleanupFailure;

  @BeforeSuite(alwaysRun = true)
  public final void resetCustomSuiteHealthBeforeSuite() {
    resetCustomSuiteHealth();
  }

  @AfterSuite(alwaysRun = true)
  public final void resetCustomSuiteHealthAfterSuite() {
    resetCustomSuiteHealth();
  }

  @BeforeClass(alwaysRun = true)
  public final void checkCustomSuiteHealthBeforeClass() {
    assertCustomSuiteHealthy();
  }

  @BeforeMethod(alwaysRun = true)
  public final void checkCustomSuiteHealthBeforeMethod() {
    assertCustomSuiteHealthy();
  }

  protected static synchronized void assertCustomSuiteHealthy() {
    if (_customSuiteCleanupFailure != null) {
      throw new IllegalStateException("A custom suite participant did not clean up successfully",
          _customSuiteCleanupFailure);
    }
  }

  protected static synchronized void recordCustomSuiteCleanupFailure(Throwable failure) {
    if (_customSuiteCleanupFailure == null) {
      _customSuiteCleanupFailure = failure;
    } else if (_customSuiteCleanupFailure != failure) {
      _customSuiteCleanupFailure.addSuppressed(failure);
    }
  }

  private static synchronized void resetCustomSuiteHealth() {
    _customSuiteCleanupFailure = null;
  }
}
