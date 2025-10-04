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

import java.util.Set;
import org.apache.pinot.spi.annotations.accounting.WorkloadBudgetManagerAnnotation;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for {@link WorkloadBudgetManager} implementations.
 */
public class WorkloadBudgetManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadBudgetManagerFactory.class);
  private static final String REGEX_PATTERN = ".*\\.spi\\.accounting\\..*";
  private static WorkloadBudgetManager _workloadBudgetManager;

  private WorkloadBudgetManagerFactory() {
  }

  /**
   * Registers WorkloadBudgetManager implementations via reflection.
   * NOTE: In order to plugin a class using reflection, the class should include ".plugin.accounting."
   * in its class path. This convention can significantly reduce the time of class scanning.
   */
  public static void register(PinotConfiguration config) {
    if (_workloadBudgetManager != null) {
      // Already registered, unregister the existing one first
      unregister();
    }
    long startTimeMs = System.currentTimeMillis();
    Set<Class<?>> classes = PinotReflectionUtils
        .getClassesThroughReflection(REGEX_PATTERN, WorkloadBudgetManagerAnnotation.class);
    String typeName = config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_WORKLOAD_BUDGET_MANAGER_TYPE_NAME,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_BUDGET_MANAGER_TYPE_NAME);
    for (Class<?> clazz : classes) {
      WorkloadBudgetManagerAnnotation annotation = clazz.getAnnotation(WorkloadBudgetManagerAnnotation.class);
      if (annotation.enabled()) {
        try {
          WorkloadBudgetManager workloadBudgetManager =
              (WorkloadBudgetManager) clazz.getConstructor(PinotConfiguration.class).newInstance(config);
          if (workloadBudgetManager.getWorkloadTypeName().equalsIgnoreCase(typeName)) {
            _workloadBudgetManager = workloadBudgetManager;
            break;
          }
        } catch (Exception e) {
          LOGGER.error("Caught exception while initializing and registering WorkloadBudgetManager: {}, skipping it",
              clazz, e);
        }
      }
    }
    // Fallback to default WorkloadBudgetManager in case of some error with resolving the class or no class found
    if (_workloadBudgetManager == null) {
      LOGGER.warn("Failed to find WorkloadBudgetManager for type: {}, falling back to default implementation",
          typeName);
      _workloadBudgetManager = new DefaultWorkloadBudgetManager(config);
    }
    LOGGER.info("Initialized WorkloadBudgetManagerRegistry with {} WorkloadBudgetManager implementations: {} in {}ms",
        classes.size(), classes, System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Returns the WorkloadBudgetManager instance registered.
   */
  public static WorkloadBudgetManager get() {
    return _workloadBudgetManager;
  }

  public static void unregister() {
    if (_workloadBudgetManager != null) {
      _workloadBudgetManager.shutdown();
      _workloadBudgetManager = null;
    }
  }
}
