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
package org.apache.pinot.controller.helix.core.ingest;

import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertExecutorFactory;


/**
 * Factory for creating {@link FileInsertExecutor} instances that handle INSERT INTO ... FROM FILE
 * statements.
 *
 * <p>This factory is registered with the broker's executor registry and is selected when the
 * {@link org.apache.pinot.spi.ingest.InsertType} is {@code FILE}. It requires references to the
 * controller's {@link PinotHelixResourceManager} (for table config lookups and segment replacement
 * protocol) and {@link PinotTaskManager} (for scheduling Minion tasks).
 *
 * <p>This class is thread-safe; a single instance is shared across broker request threads.
 */
public class FileInsertExecutorFactory implements InsertExecutorFactory {

  public static final String EXECUTOR_TYPE = "FILE";

  private PinotHelixResourceManager _resourceManager;
  private PinotTaskManager _taskManager;

  /**
   * Creates a factory instance. Call {@link #init(PinotConfiguration)} before use.
   */
  public FileInsertExecutorFactory() {
  }

  /**
   * Creates a factory instance with explicit dependencies for testing.
   *
   * @param resourceManager the Helix resource manager
   * @param taskManager     the Pinot task manager for scheduling Minion tasks
   */
  public FileInsertExecutorFactory(PinotHelixResourceManager resourceManager, PinotTaskManager taskManager) {
    _resourceManager = resourceManager;
    _taskManager = taskManager;
  }

  @Override
  public String getExecutorType() {
    return EXECUTOR_TYPE;
  }

  @Override
  public void init(PinotConfiguration config) {
    // Dependencies are injected via the constructor that takes ResourceManager and TaskManager.
    // This method is a no-op when dependencies are already set.
  }

  /**
   * Sets the resource manager. Used for wiring when the factory is created via SPI discovery
   * and dependencies are injected after construction.
   */
  public void setResourceManager(PinotHelixResourceManager resourceManager) {
    _resourceManager = resourceManager;
  }

  /**
   * Sets the task manager. Used for wiring when the factory is created via SPI discovery
   * and dependencies are injected after construction.
   */
  public void setTaskManager(PinotTaskManager taskManager) {
    _taskManager = taskManager;
  }

  @Override
  public InsertExecutor create(PinotConfiguration config) {
    if (_resourceManager == null || _taskManager == null) {
      throw new IllegalStateException(
          "FileInsertExecutorFactory requires PinotHelixResourceManager and PinotTaskManager to be set");
    }
    return new FileInsertExecutor(_resourceManager, _taskManager);
  }
}
