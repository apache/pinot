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
package org.apache.pinot.spi.utils;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for TableConfigDecorator implementations.
 * Enables external plugins to register decorators that enhance TableConfig objects.
 * By default, no decorator is registered.
 */
public class TableConfigDecoratorRegistry {
  private TableConfigDecoratorRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigDecoratorRegistry.class);

  // Default no-op decorator
  private static final TableConfigDecorator NOOP = tableConfig -> tableConfig;

  // Initialize with the no-op decorator
  private static final AtomicReference<TableConfigDecorator> DECORATOR_INSTANCE = new AtomicReference<>(NOOP);

  /**
   * Registers a decorator during startup.
   *
   * @param decorator The decorator to register
   * @return true if registration was successful, false if already registered
   */
  public static boolean register(TableConfigDecorator decorator) {
    return DECORATOR_INSTANCE.compareAndSet(NOOP, decorator);
  }

  /**
   * Applies the registered decorator to a TableConfig.
   *
   * @param tableConfig The config to decorate
   * @return The decorated config or original if decoration fails
   */
  public static TableConfig applyDecorator(TableConfig tableConfig) {
    if (tableConfig == null) {
      return null;
    }

    TableConfigDecorator decorator = DECORATOR_INSTANCE.get();
    if (decorator == null) {
      LOGGER.debug("No decorator registered, returning original TableConfig");
      return tableConfig;
    }

    try {
      return decorator.decorate(tableConfig);
    } catch (Exception e) {
      LOGGER.error("Failed to apply decorator to table config for table: {}",
          tableConfig.getTableName(), e);
      return tableConfig;
    }
  }
}
