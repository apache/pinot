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
package org.apache.pinot.minion.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link PinotTaskExecutorFactory}.
 */
public class TaskExecutorFactoryRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorFactoryRegistry.class);

  private final Map<String, PinotTaskExecutorFactory> _taskExecutorFactoryRegistry = new HashMap<>();

  /**
   * Registers the task executor factories via reflection.
   * NOTE: In order to plugin a class using reflection, the class should include ".executor." in its class path. This
   *       convention can significantly reduce the time of class scanning.
   */
  public TaskExecutorFactoryRegistry(MinionTaskZkMetadataManager zkMetadataManager) {
    long startTimeMs = System.currentTimeMillis();
    Reflections reflections = new Reflections(
        new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
            .filterInputsBy(new FilterBuilder.Include(".*\\.executor\\..*")).setScanners(new TypeAnnotationsScanner()));
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(TaskExecutorFactory.class, true);
    for (Class<?> clazz : classes) {
      TaskExecutorFactory annotation = clazz.getAnnotation(TaskExecutorFactory.class);
      if (annotation.enabled()) {
        try {
          PinotTaskExecutorFactory taskExecutorFactory = (PinotTaskExecutorFactory) clazz.newInstance();
          taskExecutorFactory.init(zkMetadataManager);
          registerTaskExecutorFactory(taskExecutorFactory);
        } catch (Exception e) {
          LOGGER.error("Caught exception while initializing and registering task executor factory: {}, skipping it",
              clazz, e);
        }
      }
    }
    LOGGER.info("Initialized TaskExecutorFactoryRegistry with {} task executor factories: {} in {}ms",
        _taskExecutorFactoryRegistry.size(), _taskExecutorFactoryRegistry.keySet(),
        System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Registers a task executor factory.
   */
  public void registerTaskExecutorFactory(PinotTaskExecutorFactory taskExecutorFactory) {
    _taskExecutorFactoryRegistry.put(taskExecutorFactory.getTaskType(), taskExecutorFactory);
  }

  /**
   * Returns all registered task types.
   */
  public Set<String> getAllTaskTypes() {
    return _taskExecutorFactoryRegistry.keySet();
  }

  /**
   * Returns the task executor factory for the given task type.
   */
  public PinotTaskExecutorFactory getTaskExecutorFactory(String taskType) {
    return _taskExecutorFactoryRegistry.get(taskType);
  }
}
