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
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class UnboundedResourceManagerTest {

  @Test
  public void testDefault() {
    UnboundedResourceManager rm = new UnboundedResourceManager(new PinotConfiguration());
    assertTrue(rm.getNumQueryRunnerThreads() > 1);
    assertTrue(rm.getNumQueryWorkerThreads() >= 1);
    assertEquals(rm.getTableThreadsHardLimit(), rm.getNumQueryRunnerThreads() + rm.getNumQueryWorkerThreads());
    assertEquals(rm.getTableThreadsSoftLimit(), rm.getNumQueryRunnerThreads() + rm.getNumQueryWorkerThreads());
  }

  @Test
  public void testWithConfig() {
    Map<String, Object> properties = new HashMap<>();
    final int workers = 5;
    final int runners = 2;

    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);

    UnboundedResourceManager rm = new UnboundedResourceManager(new PinotConfiguration(properties));
    assertEquals(rm.getNumQueryWorkerThreads(), workers);
    assertEquals(rm.getNumQueryRunnerThreads(), runners);
    assertEquals(rm.getTableThreadsHardLimit(), runners + workers);
    assertEquals(rm.getTableThreadsSoftLimit(), runners + workers);

    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    when(accountant.totalReservedThreads()).thenReturn(3);
    assertTrue(rm.canSchedule(accountant));

    when(accountant.totalReservedThreads()).thenReturn(workers + runners + 2);
    assertFalse(rm.canSchedule(accountant));
  }
}
