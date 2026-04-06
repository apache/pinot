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
package org.apache.pinot.core.query.killing;

import javax.annotation.Nullable;
import org.apache.pinot.core.accounting.QueryMonitorConfig;


/**
 * Factory for creating {@link QueryKillingStrategy} instances from config.
 *
 * <p>Implementations are loaded by class name via the config key
 * {@code accounting.scan.based.killing.strategy.factory.class.name}.
 * Each factory reads whatever config keys it needs from {@link QueryMonitorConfig}
 * and creates a strategy instance. If required config is missing, the factory
 * returns {@code null} and logs a warning.</p>
 *
 * To add a new strategy:
 * <ol>
 *   <li>Implement {@link QueryKillingStrategy} with your kill logic</li>
 *   <li>Implement this interface to create your strategy from config</li>
 *   <li>Set the config key to your factory class name</li>
 *   <li>Add your strategy-specific config keys to the server config</li>
 * </ol>
 *
 * @see org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy.Factory
 */
public interface QueryKillingStrategyFactory {

  /**
   * Creates a strategy from the current config. Returns {@code null} if required
   * configuration is not present (e.g., no thresholds set), in which case the
   * manager will log a warning and scan-based killing will be effectively disabled.
   */
  @Nullable
  QueryKillingStrategy create(QueryMonitorConfig config);

  /**
   * Human-readable name of this factory for logging.
   */
  String getName();
}
