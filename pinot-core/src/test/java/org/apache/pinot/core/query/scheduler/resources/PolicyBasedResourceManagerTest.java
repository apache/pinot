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
package org.apache.pinot.core.query.scheduler.resources;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class PolicyBasedResourceManagerTest {
  private PolicyBasedResourceManager _resourceManager;

  @AfterMethod
  public void tearDown() {
    if (_resourceManager != null) {
      _resourceManager.stop();
      _resourceManager = null;
    }
  }

  @Test
  public void testResizeUpdatesResourceLimitPolicy() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, 4);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, 20);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 50);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 30);
    _resourceManager = new PolicyBasedResourceManager(new PinotConfiguration(properties));

    int originalHardLimit = _resourceManager.getTableThreadsHardLimit();
    int originalSoftLimit = _resourceManager.getTableThreadsSoftLimit();

    _resourceManager.resizeThreadPools(8, 40);

    assertNotEquals(_resourceManager.getTableThreadsHardLimit(), originalHardLimit);
    assertNotEquals(_resourceManager.getTableThreadsSoftLimit(), originalSoftLimit);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 40);
  }

  @Test
  public void testResizeDoesNotUpdatePolicyOnNoChange() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, 4);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, 20);
    _resourceManager = new PolicyBasedResourceManager(new PinotConfiguration(properties));

    int originalHardLimit = _resourceManager.getTableThreadsHardLimit();
    int originalSoftLimit = _resourceManager.getTableThreadsSoftLimit();

    _resourceManager.resizeThreadPools(4, 20);

    assertEquals(_resourceManager.getTableThreadsHardLimit(), originalHardLimit);
    assertEquals(_resourceManager.getTableThreadsSoftLimit(), originalSoftLimit);
  }
}
