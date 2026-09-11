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

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;


/// Shared table-mode safety validator used by both {@link ControllerRowInsertExecutor} and
/// {@link FileInsertExecutor}. Centralizing here ensures a single source of truth for the rules
/// that gate INSERT INTO against unsafe table modes (upsert/dedup/multi-column partition); a future
/// rule change lands in one place rather than diverging across executors.
///
/// Rules in v1:
/// - Materialized view tables: rejected (MV data is computed from base tables via
///       MaterializedViewTask; direct INSERT would break the MV invariant).
/// - Dedup tables: rejected (no per-row primary-key dedup at controller-side insert).
/// - Partial upsert: rejected (requires stream ingestion).
/// - Full upsert: requires explicit segment partition config (single-column).
/// - Multi-column partition config: rejected (segment-per-row-partition routing is single-column).
final class InsertTableModeValidator {
  private InsertTableModeValidator() {
  }

  /// Validates the given table config against INSERT-INTO safety rules.
  ///
  /// @param tableConfig the table configuration
  /// @param insertKindLabel "row" or "file" — used in error messages so callers see which
  ///     executor they tripped against
  /// @return an error message if the table mode is incompatible, or `null` if allowed
  @Nullable
  static String validate(TableConfig tableConfig, String insertKindLabel) {
    /// Materialized views are computed from base tables by MaterializedViewTask; a direct INSERT
    /// would write rows that do not exist in the source data, violating the MV invariant. Reject
    /// up front before any executor-side validation runs.
    if (tableConfig.isMaterializedView()) {
      return "Materialized view tables do not support direct " + insertKindLabel + " insert; "
          + "MV data is computed from base tables.";
    }
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    if (dedupConfig != null && dedupConfig.isDedupEnabled()) {
      return "Dedup tables do not support direct " + insertKindLabel + " insert in this version.";
    }
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE) {
      if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
        return "Partial upsert tables do not support direct " + insertKindLabel + " insert. "
            + "Use stream ingestion for partial upsert.";
      }
      if (upsertConfig.getMode() == UpsertConfig.Mode.FULL) {
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        if (indexingConfig == null) {
          return "Full upsert table requires explicit partition configuration for " + insertKindLabel
              + " insert, but no indexing config found.";
        }
        SegmentPartitionConfig partitionConfig = indexingConfig.getSegmentPartitionConfig();
        if (partitionConfig == null || partitionConfig.getColumnPartitionMap() == null
            || partitionConfig.getColumnPartitionMap().isEmpty()) {
          return "Full upsert table requires explicit partition configuration for " + insertKindLabel
              + " insert. Configure segmentPartitionConfig in the table's indexing config.";
        }
      }
    }
    /// INSERT INTO does not support multi-column partition configs; reject up front so callers see
    /// TABLE_MODE_REJECTED instead of a confusing PARTITION_VALUE_REJECTED mid-execution.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    SegmentPartitionConfig partitionConfig = indexingConfig != null
        ? indexingConfig.getSegmentPartitionConfig() : null;
    if (partitionConfig != null && partitionConfig.getColumnPartitionMap() != null
        && partitionConfig.getColumnPartitionMap().size() > 1) {
      return "INSERT INTO does not support multi-column partition configs. Found partition columns: "
          + partitionConfig.getColumnPartitionMap().keySet();
    }
    return null;
  }
}
