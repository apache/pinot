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
package org.apache.pinot.controller.helix.core.minion.generator;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link PinotTaskGenerator}.
 */
public class TaskGeneratorRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskGeneratorRegistry.class);

  private final Map<String, PinotTaskGenerator> _taskGeneratorRegistry = new HashMap<>();

  /**
   * The package regex pattern for auto-registered {@link PinotTaskGenerator}.
   */
  public static final String TASK_GENERATOR_PACKAGE_REGEX_PATTERN = ".*\\.plugin\\.minion\\.tasks\\..*";

  /**
   * Registers the task generators via reflection.
   * NOTE: In order to plugin a class using reflection, the class should include ".plugin.minion.tasks." in its class
   * path. This convention can significantly reduce the time of class scanning.
   */
  public TaskGeneratorRegistry(ClusterInfoAccessor clusterInfoAccessor) {
    long startTimeMs = System.currentTimeMillis();
    Set<Class<?>> classes = getTaskGeneratorClasses();
    for (Class<?> clazz : classes) {
      TaskGenerator annotation = clazz.getAnnotation(TaskGenerator.class);
      if (annotation.enabled()) {
        try {
          PinotTaskGenerator taskGenerator = (PinotTaskGenerator) clazz.newInstance();
          taskGenerator.init(clusterInfoAccessor);
          registerTaskGenerator(taskGenerator);
        } catch (Exception e) {
          LOGGER.error("Caught exception while initializing and registering task generator: {}, skipping it", clazz, e);
        }
      }
    }
    LOGGER.info("Initialized TaskGeneratorRegistry with {} task generators: {} in {}ms", _taskGeneratorRegistry.size(),
        _taskGeneratorRegistry.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  public static Set<Class<?>> getTaskGeneratorClasses() {
    return PinotReflectionUtils.getClassesThroughReflection(TASK_GENERATOR_PACKAGE_REGEX_PATTERN, TaskGenerator.class);
  }

  /**
   * Register a task generator.
   */
  public void registerTaskGenerator(PinotTaskGenerator pinotTaskGenerator) {
    // Task type cannot contain the task name separator
    String taskType = pinotTaskGenerator.getTaskType();
    Preconditions.checkArgument(!taskType.contains(PinotHelixTaskResourceManager.TASK_NAME_SEPARATOR),
        "Task type: %s cannot contain underscore character", taskType);

    _taskGeneratorRegistry.put(taskType, pinotTaskGenerator);
  }

  /**
   * Returns all registered task types.
   */
  public Set<String> getAllTaskTypes() {
    return _taskGeneratorRegistry.keySet();
  }

  /**
   * Returns the task generator for the given task type.
   */
  @Nullable
  public PinotTaskGenerator getTaskGenerator(String taskType) {
    return _taskGeneratorRegistry.get(taskType);
  }
}
