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
package org.apache.pinot.materializedview.context;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Role-neutral context needed by the materialized-view analyzer and task scheduler.
///
/// The minion task plugin provides the controller-backed implementation. The analyzer and
/// scheduler remain in this module and only depend on this narrow contract.
///
/// Existence-probing is explicit: callers MUST invoke [#tableExists] before
/// [#getTableConfig] / [#getTableSchema] if the table is not already known to exist
/// (e.g. the analyzer's OFFLINE→REALTIME source-table fallback). The latter two methods
/// throw `IllegalStateException` on absence rather than returning null, so consumer-site
/// call paths can never accidentally swallow a missing-entity error.
public interface MaterializedViewTaskGeneratorContext {

  /// Returns `true` when a table with the given fully-qualified name exists in cluster metadata.
  /// Used by the analyzer's source-table resolver to probe both OFFLINE and REALTIME variants
  /// at MV-create time before committing to one form.
  boolean tableExists(String tableNameWithType);

  /// Returns the table config for the given fully-qualified table name (with `_OFFLINE` or
  /// `_REALTIME` suffix). Throws `IllegalStateException` when the table does not exist.
  ///
  /// Callers that need to probe for existence MUST use [#tableExists] first; this method
  /// assumes the caller has already established the table exists. Base-table delete is
  /// blocked at the controller when dependent MVs exist (see
  /// `MaterializedViewConsistencyManager#getDependentMaterializedViews`), so once the MV
  /// passes create-time validation, downstream consumers (scheduler, executor, consistency
  /// manager) can rely on the source table being present.
  TableConfig getTableConfig(String tableNameWithType);

  /// Returns the schema for the given table. Schemas in Pinot are stored by raw table name;
  /// for caller convenience implementations MUST accept either form (raw name or
  /// name-with-type suffix) and strip the suffix internally before lookup.
  ///
  /// Throws `IllegalStateException` when no schema is registered for the table. The analyzer
  /// invokes this on a name it has already confirmed exists; the throw signals a cluster-state
  /// inconsistency (e.g. an aborted table-create that registered the config but not the
  /// schema) which must fail the operation rather than be silently null-handled downstream.
  Schema getTableSchema(String tableName);

  HelixPropertyStore<ZNRecord> getPropertyStore();

  List<SegmentZKMetadata> getSegmentsZKMetadata(String tableNameWithType);

  String getVipUrl();

  void forRunningTasks(String tableNameWithType, String taskType, Consumer<Map<String, String>> taskConfigConsumer);

  /// Returns the live value of the given Helix cluster-config key, or `null` if unset.
  ///
  /// Implementations MUST read the value on every call (no caching) so an operator updating
  /// the cluster config sees the change applied without a controller / minion restart.
  /// Callers parse the returned string and apply their own default on null / malformed values.
  @Nullable
  String getClusterConfig(String configName);
}
