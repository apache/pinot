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
package org.apache.pinot.query.planner.spi.stats;

import java.util.Map;


/// Creates a [StatsStore] for a configured name, discovered through [java.util.ServiceLoader].
///
/// Implementations declare their own name, so operators configure a stable identifier rather than a
/// class name: renaming or moving the implementation must not invalidate an operator's
/// configuration. Register one by listing it in
/// `META-INF/services/org.apache.pinot.query.planner.spi.stats.StatsStoreProvider`.
///
/// Thread-safety: providers are discovered once and shared, so implementations must be thread-safe.
public interface StatsStoreProvider {

  /// Returns the identifier operators use to select this store. Must be unique across all providers
  /// on the classpath and stable across releases, since it appears in configuration.
  String getName();

  /// Creates a store; the caller initializes it via [StatsStore#init()].
  ///
  /// @param properties statistics-related broker configuration, with keys stripped of the
  ///                   `pinot.broker.stats.` prefix (for example `dir`)
  /// @throws StatsStoreException if the store cannot be created from this configuration
  StatsStore create(Map<String, String> properties)
      throws StatsStoreException;
}
