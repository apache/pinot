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
package org.apache.pinot.minion.event;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.spi.annotations.minion.EventObserverFactory;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link MinionEventObserverFactory}.
 */
public class EventObserverFactoryRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventObserverFactoryRegistry.class);

  private final Map<String, MinionEventObserverFactory> _eventObserverFactoryRegistry = new HashMap<>();

  /**
   * Registers the event observer factories via reflection.
   * NOTE: In order to plugin a class using reflection, the class should include ".event." in its class path. This
   *       convention can significantly reduce the time of class scanning.
   */
  public EventObserverFactoryRegistry(MinionTaskZkMetadataManager zkMetadataManager) {
    long startTimeMs = System.currentTimeMillis();
    Reflections reflections = new Reflections(
        new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
            .filterInputsBy(new FilterBuilder.Include(".*\\.event\\..*")).setScanners(new TypeAnnotationsScanner()));
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(EventObserverFactory.class, true);
    for (Class<?> clazz : classes) {
      EventObserverFactory annotation = clazz.getAnnotation(EventObserverFactory.class);
      if (annotation.enabled()) {
        try {
          MinionEventObserverFactory eventObserverFactory = (MinionEventObserverFactory) clazz.newInstance();
          eventObserverFactory.init(zkMetadataManager);
          registerEventObserverFactory(eventObserverFactory);
        } catch (Exception e) {
          LOGGER.error("Caught exception while initializing and registering event observer factory: {}, skipping it",
              clazz, e);
        }
      }
    }
    LOGGER.info("Initialized EventObserverFactoryRegistry with {} event observer factories: {} in {}ms",
        _eventObserverFactoryRegistry.size(), _eventObserverFactoryRegistry.keySet(),
        System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Registers an event observer factory.
   */
  public void registerEventObserverFactory(MinionEventObserverFactory eventObserverFactory) {
    _eventObserverFactoryRegistry.put(eventObserverFactory.getTaskType(), eventObserverFactory);
  }

  /**
   * Returns the event observer factory for the given task type, or default event observer if no one is registered.
   */
  public MinionEventObserverFactory getEventObserverFactory(String taskType) {
    return _eventObserverFactoryRegistry.getOrDefault(taskType, DefaultMinionEventObserverFactory.getInstance());
  }
}
