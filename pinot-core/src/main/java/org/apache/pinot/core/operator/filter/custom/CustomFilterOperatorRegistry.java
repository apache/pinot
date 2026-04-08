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
package org.apache.pinot.core.operator.filter.custom;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for custom filter operator factories.
 *
 * <p>Plugins register a {@link CustomFilterOperatorFactory} that creates filter operators
 * for custom predicate types at query execution time. Factories are discovered via
 * {@link ServiceLoader} during {@link #init()}, or registered programmatically.
 *
 * <p>This registry is consulted by {@code FilterPlanNode} when a predicate of type
 * {@code CUSTOM} is encountered.
 */
public class CustomFilterOperatorRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomFilterOperatorRegistry.class);
  private static final Map<String, CustomFilterOperatorFactory> REGISTRY = new ConcurrentHashMap<>();

  private CustomFilterOperatorRegistry() {
  }

  /**
   * Discovers and registers all {@link CustomFilterOperatorFactory} implementations via ServiceLoader.
   */
  public static void init() {
    ServiceLoader<CustomFilterOperatorFactory> loader = ServiceLoader.load(CustomFilterOperatorFactory.class);
    for (CustomFilterOperatorFactory factory : loader) {
      register(factory);
    }
  }

  /**
   * Programmatically registers a custom filter operator factory.
   *
   * @param factory the factory to register
   * @throws IllegalStateException if a different factory is already registered for the same predicate name
   */
  public static void register(CustomFilterOperatorFactory factory) {
    String name = factory.predicateName().toUpperCase();
    CustomFilterOperatorFactory existing = REGISTRY.putIfAbsent(name, factory);
    if (existing != null && existing != factory) {
      throw new IllegalStateException(
          String.format("Cannot register CustomFilterOperatorFactory '%s' (%s): already registered by %s",
              name, factory.getClass().getName(), existing.getClass().getName()));
    }
    LOGGER.info("Registered custom filter operator factory: {} ({})", name, factory.getClass().getName());
  }

  /**
   * Returns the factory for the given predicate name, or null if none is registered.
   */
  @Nullable
  public static CustomFilterOperatorFactory get(String predicateName) {
    return REGISTRY.get(predicateName.toUpperCase());
  }

  /**
   * Returns true if a factory is registered for the given predicate name.
   */
  public static boolean isRegistered(String predicateName) {
    return REGISTRY.containsKey(predicateName.toUpperCase());
  }

  /**
   * Removes all registered factories. Intended for testing only.
   */
  public static void clear() {
    REGISTRY.clear();
  }
}
