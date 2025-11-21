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
package org.apache.pinot.spi.accounting;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.annotations.accounting.WorkloadBudgetManagerAnnotation;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class WorkloadBudgetManagerFactoryTest {

  /**
   * Custom test WorkloadBudgetManager implementation that should be discovered by reflection.
   * This class is placed in a package path that matches the reflection pattern ".*\.plugin\.workload\..*"
   */
  @WorkloadBudgetManagerAnnotation
  public static class TestCustomWorkloadBudgetManager extends DefaultWorkloadBudgetManager {
    public TestCustomWorkloadBudgetManager(PinotConfiguration config) {
      super(config);
    }

    @Override
    public String getWorkloadTypeName() {
      return "test-custom";
    }

    @Override
    public void collectWorkloadStats(String workload, WorkloadBudgetManager.BudgetStats stats) {
      // Custom implementation for testing
      super.collectWorkloadStats(workload, stats);
    }
  }

  @Test
  public void testDefaultWorkloadBudgetManagerWhenNoCustomType() {
    // Test that default WorkloadBudgetManager is returned when no custom type is specified
    Map<String, Object> configMap = new HashMap<>();
    PinotConfiguration config = new PinotConfiguration(configMap);
    WorkloadBudgetManagerFactory.unregister();
    WorkloadBudgetManagerFactory.register(config);
    WorkloadBudgetManager manager = WorkloadBudgetManagerFactory.get();

    assertNotNull(manager, "WorkloadBudgetManager should not be null");
    assertEquals(manager.getWorkloadTypeName(),
        CommonConstants.Accounting.DEFAULT_WORKLOAD_BUDGET_MANAGER_TYPE_NAME,
        "Should return default workload budget manager type name");
  }

  @Test
  public void testCustomWorkloadBudgetManagerType() {
    // Test requesting a specific custom WorkloadBudgetManager type
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_BUDGET_MANAGER_TYPE_NAME, "test-custom");
    PinotConfiguration config = new PinotConfiguration(configMap);

    WorkloadBudgetManagerFactory.register(config);
    WorkloadBudgetManager manager = WorkloadBudgetManagerFactory.get();
    assertNotNull(manager, "WorkloadBudgetManager should not be null");
    assertEquals(manager.getWorkloadTypeName(), "test-custom",
        "Should return the custom workload budget manager type name");
  }
}
