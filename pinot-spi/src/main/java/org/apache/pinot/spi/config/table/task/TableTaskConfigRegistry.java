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
package org.apache.pinot.spi.config.table.task;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.annotations.table.TableTaskConfig;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link TableTaskTypeConfig}.
 */
public class TableTaskConfigRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableTaskConfigRegistry.class);

  /**
   * The package regex pattern for auto-registered {@link TableTaskTypeConfig}.
   */
  private static final String TABLE_TASK_CONFIG_PACKAGE_REGEX_PATTERN = ".*\\.plugin\\.config\\.table\\.task\\..*";

  private final Map<String, Class<? extends TableTaskTypeConfig>> _tableTaskConfigRegistry = new HashMap<>();

  /**
   * Registers the table task type configurations via reflection.
   * NOTE: In order to plugin a class using reflection, the class should include ".plugin.config.table.task" in its class
   * path. This convention can significantly reduce the time of class scanning.
   */
  public TableTaskConfigRegistry() {
    long startTimeMs = System.currentTimeMillis();
    Set<Class<?>> classes = getTableTaskConfigClasses();
    for (Class<?> clazz : classes) {
      TableTaskConfig annotation = clazz.getAnnotation(TableTaskConfig.class);
      if (annotation.enabled()) {
        try {
          // Validate that the class extends TableTaskTypeConfig
          if (!TableTaskTypeConfig.class.isAssignableFrom(clazz)) {
            LOGGER.error("Class {} does not extend TableTaskTypeConfig, skipping registration", clazz);
            continue;
          }

          @SuppressWarnings("unchecked")
          Class<? extends TableTaskTypeConfig> taskConfigClass = (Class<? extends TableTaskTypeConfig>) clazz;

          // Extract task type from class name (e.g., SegmentPurgeTaskConfig -> SegmentPurgeTask)
          String taskType = getTaskTypeFromClassName(clazz.getSimpleName());
          registerTableTaskConfig(taskType, taskConfigClass);
        } catch (Exception e) {
          LOGGER.error("Caught exception while registering table task config: {}, skipping it", clazz, e);
        }
      }
    }
    LOGGER.info("Initialized TableTaskConfigRegistry with {} table task configs: {} in {}ms",
        _tableTaskConfigRegistry.size(), _tableTaskConfigRegistry.keySet(),
        System.currentTimeMillis() - startTimeMs);
  }

  public static Set<Class<?>> getTableTaskConfigClasses() {
    return PinotReflectionUtils
        .getClassesThroughReflection(TABLE_TASK_CONFIG_PACKAGE_REGEX_PATTERN, TableTaskConfig.class);
  }

  /**
   * Registers a table task type configuration class.
   */
  public void registerTableTaskConfig(String taskType, Class<? extends TableTaskTypeConfig> taskConfigClass) {
    _tableTaskConfigRegistry.put(taskType, taskConfigClass);
  }

  /**
   * Returns all registered task types.
   */
  public Set<String> getAllTaskTypes() {
    return _tableTaskConfigRegistry.keySet();
  }

  /**
   * Returns the table task config class for the given task type.
   */
  public Class<? extends TableTaskTypeConfig> getTableTaskConfigClass(String taskType) {
    return _tableTaskConfigRegistry.get(taskType);
  }

  /**
   * Creates a table task config instance for the given task type with the provided configuration.
   */
  public TableTaskTypeConfig createTableTaskConfig(String taskType, Map<String, String> configs) {
    Class<? extends TableTaskTypeConfig> taskConfigClass = _tableTaskConfigRegistry.get(taskType);
    if (taskConfigClass == null) {
      return null;
    }

    try {
      Constructor<? extends TableTaskTypeConfig> constructor =
          taskConfigClass.getDeclaredConstructor(Map.class);
      constructor.setAccessible(true);
      return constructor.newInstance(configs);
    } catch (Exception e) {
      LOGGER.error("Failed to create table task config for task type: {}", taskType, e);
      return null;
    }
  }

  /**
   * Extracts task type from class name.
   * E.g., "SegmentRefreshTaskConfig" -> "SegmentRefreshTask"
   */
  private String getTaskTypeFromClassName(String className) {
    if (className.endsWith("Config")) {
      return className.substring(0, className.length() - 6); // Remove "Config" suffix
    }
    return className;
  }
}
