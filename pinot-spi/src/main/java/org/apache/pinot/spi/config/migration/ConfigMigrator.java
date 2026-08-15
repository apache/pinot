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
/// Each migrator advances a config exactly one version: it accepts an input at version
/// [#fromVersion()] and returns an equivalent config at version {@code fromVersion() + 1}. The
/// [ConfigMigrationRegistry] composes an ordered list of migrators to bring a config from any
/// stored version up to the current version.
///
/// Implementations must be:
/// - **Pure with respect to versioning**: a migrator for {@code fromVersion() == N} must only ever
///   run against configs at version {@code N}.
/// - **Idempotent-safe**: the registry never runs a migrator against a config already at or beyond
///   {@code fromVersion() + 1}, so implementations only need to handle the version they declare.
/// - **Thread-safe**: migrators are singletons registered once at startup and may be invoked
///   concurrently for different tables.
///
/// @param <T> the configuration type being migrated (e.g. {@code TableConfig} or {@code Schema})
public interface ConfigMigrator<T> {

  /// The migration version this migrator upgrades **from**. The result of [#migrate(Object)] is at
  /// version {@code fromVersion() + 1}. Versions are dense, non-negative integers starting at 0
  /// (0 represents a config written before the migration framework existed).
  int fromVersion();

  /// Upgrades the given config from [#fromVersion()] to {@code fromVersion() + 1}.
  ///
  /// Implementations may either mutate {@code input} in place and return it, or return a new
  /// object. The version marker itself is stamped by the registry, not by the migrator.
  ///
  /// @param input config at version [#fromVersion()]
  /// @return an equivalent config at version {@code fromVersion() + 1}
  T migrate(T input);
}
