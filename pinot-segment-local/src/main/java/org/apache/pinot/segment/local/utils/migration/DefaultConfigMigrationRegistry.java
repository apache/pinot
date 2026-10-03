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
package org.apache.pinot.segment.local.utils.migration;

import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;


/// Assembles the built-in [ConfigMigrationRegistry] with the migration chain shipped by Pinot.
///
/// This is the single place that declares the ordered set of migrators, and therefore defines the
/// current table-config and schema migration versions. Add a new migrator to the end of the relevant
/// chain (with `fromVersion` equal to the current chain length) to introduce a new upgrade step.
///
/// Table-config chain:
/// - v0 -> v1: [LegacyIngestionConfigMigrator] (fold deprecated ingestion fields into ingestionConfig)
///
/// Schema chain:
/// - (none yet) The framework is wired end-to-end; the current schema version is 0, so schemas are
///   never rewritten until a real schema migrator is added here.
public class DefaultConfigMigrationRegistry {
  private DefaultConfigMigrationRegistry() {
  }

  /// Builds a fresh registry populated with the built-in migration chain.
  public static ConfigMigrationRegistry create() {
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    registry.registerTableConfigMigrator(new LegacyIngestionConfigMigrator());
    // No schema migrators yet; add them here (fromVersion == current schema chain length) as needed.
    return registry;
  }
}
