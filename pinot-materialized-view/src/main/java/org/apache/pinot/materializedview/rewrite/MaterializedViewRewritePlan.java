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
package org.apache.pinot.materializedview.rewrite;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;


/// Structured output of the MV rewrite layer, separating static matching
/// ([MatchType]) from runtime execution decisions ([ExecutionMode]).
///
/// Strategies produce plan fragments with `execMode = null`. The
/// [MaterializedViewQueryRewriteEngine] then resolves execution mode and
/// populates the split-mode runtime fields via
/// [#withExecMode(ExecutionMode, MaterializedViewSplitSpec, long)].
///
/// Lower cost is better. The rewrite engine picks the plan with the lowest
/// cost when multiple MVs match a single user query.
public class MaterializedViewRewritePlan implements Comparable<MaterializedViewRewritePlan> {

  private final String _materializedViewTableNameWithType;
  private final MatchType _matchType;
  @Nullable
  private final ExecutionMode _execMode;
  private final PinotQuery _materializedViewQuery;
  private final double _cost;

  // Split-mode runtime fields — populated by the engine during plan resolution,
  // not by strategies. Only meaningful when execMode == SPLIT_REWRITE.
  @Nullable
  private final MaterializedViewSplitSpec _splitSpec;
  private final long _watermarkMs;

  /// Whether this plan can be used in SPLIT_REWRITE mode. False for AGG_REAGG
  /// plans that use non-distributive re-aggregation rules (e.g. COUNT->SUM)
  /// because the MV side produces intermediate types incompatible with what the
  /// base-side reducer expects.
  private final boolean _splitSafe;

  public MaterializedViewRewritePlan(String viewTableNameWithType, MatchType matchType,
      @Nullable ExecutionMode execMode, PinotQuery viewQuery, double cost) {
    this(viewTableNameWithType, matchType, execMode, viewQuery, cost, null, 0, true);
  }

  public MaterializedViewRewritePlan(String viewTableNameWithType, MatchType matchType,
      @Nullable ExecutionMode execMode, PinotQuery viewQuery, double cost, boolean splitSafe) {
    this(viewTableNameWithType, matchType, execMode, viewQuery, cost, null, 0, splitSafe);
  }

  public MaterializedViewRewritePlan(String viewTableNameWithType, MatchType matchType,
      @Nullable ExecutionMode execMode, PinotQuery viewQuery, double cost,
      @Nullable MaterializedViewSplitSpec splitSpec, long watermarkMs, boolean splitSafe) {
    _materializedViewTableNameWithType = viewTableNameWithType;
    _matchType = matchType;
    _execMode = execMode;
    _materializedViewQuery = viewQuery;
    _cost = cost;
    _splitSpec = splitSpec;
    _watermarkMs = watermarkMs;
    _splitSafe = splitSafe;
  }

  /// Returns a new plan with execution mode + split runtime parameters set, preserving all other
  /// fields from this plan.  The broker's split execution path uses
  /// [#getMaterializedViewQuery()] as the MV-side template and the pre-rewrite
  /// `compileResult._serverPinotQuery` as the base-side template — no separate base-query copy is
  /// retained on the plan to avoid an O(query-size) `deepCopy()` per candidate.
  public MaterializedViewRewritePlan withExecMode(ExecutionMode execMode,
      @Nullable MaterializedViewSplitSpec splitSpec, long watermarkMs) {
    return new MaterializedViewRewritePlan(_materializedViewTableNameWithType, _matchType, execMode,
        _materializedViewQuery, _cost, splitSpec, watermarkMs, _splitSafe);
  }

  public String getMaterializedViewTableNameWithType() {
    return _materializedViewTableNameWithType;
  }

  public MatchType getMatchType() {
    return _matchType;
  }

  @Nullable
  public ExecutionMode getExecMode() {
    return _execMode;
  }

  public PinotQuery getMaterializedViewQuery() {
    return _materializedViewQuery;
  }

  public double getCost() {
    return _cost;
  }

  @Nullable
  public MaterializedViewSplitSpec getSplitSpec() {
    return _splitSpec;
  }

  /// Watermark from the MV's runtime metadata.  Under Design C this is the split point for
  /// SPLIT_REWRITE: base side `time >= watermarkMs`, MV side `time < watermarkMs`.
  public long getWatermarkMs() {
    return _watermarkMs;
  }

  public boolean isSplitSafe() {
    return _splitSafe;
  }

  @Override
  public int compareTo(MaterializedViewRewritePlan other) {
    return Double.compare(_cost, other._cost);
  }
}
