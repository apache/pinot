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
package org.apache.pinot.spi.ingest;

import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Factory for creating {@link InsertExecutor} instances.
 *
 * <p>Each factory handles a specific executor type (e.g., "ROW" or "FILE"). The broker discovers
 * registered factories and delegates to the appropriate one based on the {@link InsertType} of the
 * request.
 *
 * <p>Implementations must be thread-safe. The {@link #init(PinotConfiguration)} method is called
 * once during startup before any calls to {@link #create(PinotConfiguration)}.
 */
public interface InsertExecutorFactory {

  /**
   * Returns the executor type this factory handles (e.g., "ROW", "FILE").
   */
  String getExecutorType();

  /**
   * Initializes this factory with the given configuration. Called once during startup.
   *
   * @param config the Pinot configuration
   */
  void init(PinotConfiguration config);

  /**
   * Creates a new {@link InsertExecutor} instance with the given configuration.
   *
   * @param config the Pinot configuration
   * @return a new executor instance
   */
  InsertExecutor create(PinotConfiguration config);
}
