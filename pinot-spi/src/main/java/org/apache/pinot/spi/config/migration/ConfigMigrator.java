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
package org.apache.pinot.spi.config.migration;

/// A single step in a versioned migration chain that upgrades a configuration object from one
/// migration version to the next.
///
/// [#fromVersion()] defines the step's position in the ordered chain. The [ConfigMigrationRegistry]
/// reruns the chain for configs at any supported version, because later user updates can reintroduce
/// legacy properties without clearing the version marker.
///
/// Implementations must be:
/// - **Idempotent**: a current-format input must remain unchanged, even after repeated execution.
/// - **Compatible with later steps**: legacy properties may already have been modernized by a previous run.
/// - **Thread-safe**: migrators are singletons registered once at startup and may be invoked
///   concurrently for different tables.
///
/// @param <T> the configuration type being migrated (e.g. {@code TableConfig} or {@code Schema})
public interface ConfigMigrator<T> {

  /// The step's position in the migration chain. Positions are dense, non-negative integers starting at 0.
  /// The registry stamps the chain's target version only after a substantive transformation.
  int fromVersion();

  /// Modernizes any legacy properties owned by this step, leaving current-format input unchanged.
  ///
  /// Implementations may either mutate {@code input} in place and return it, or return a new
  /// object. The version marker itself is stamped by the registry, not by the migrator.
  ///
  /// @param input config at any supported migration version
  /// @return the equivalent modernized config
  T migrate(T input);
}
