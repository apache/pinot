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
package org.apache.pinot.materializedview.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.materializedview.context.MaterializedViewContext;
import org.apache.pinot.materializedview.context.MaterializedViewContext.SplitRewriteContext;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.rewrite.ExecutionMode;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryRewriteEngine;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryRewriteEngineFactory;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewriteResult;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Default [MaterializedViewHandler] backed by a [MaterializedViewQueryRewriteEngine].
///
/// Owns all MV-specific compile-time decisions and split-execution time-boundary attachment, so
/// the broker request handler only needs to invoke the SPI hooks and provide a dispatcher
/// callback for the generic route-build + scatter-gather + reduce work.
///
/// Thread-safe: shares the underlying rewrite engine (which is itself thread-safe) across all
/// request threads.
public class DefaultMaterializedViewHandler implements MaterializedViewHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMaterializedViewHandler.class);

  private MaterializedViewQueryRewriteEngine _rewriteEngine;
  private boolean _supportsSplitRewrite;

  /// Public no-arg constructor required by [MaterializedViewHandler#loadHandler]. State is
  /// populated by [#init] or by the test-only convenience constructors below.
  public DefaultMaterializedViewHandler() {
  }

  /// Test-only: pre-built rewrite engine + explicit split-rewrite capability.
  @VisibleForTesting
  public DefaultMaterializedViewHandler(MaterializedViewQueryRewriteEngine rewriteEngine,
      boolean supportsSplitRewrite) {
    _rewriteEngine = rewriteEngine;
    _supportsSplitRewrite = supportsSplitRewrite;
  }

  /// Test-only: pre-built rewrite engine; defaults split-rewrite support to `true`.
  @VisibleForTesting
  public DefaultMaterializedViewHandler(MaterializedViewQueryRewriteEngine rewriteEngine) {
    this(rewriteEngine, true);
  }

  @Override
  public void init(PinotConfiguration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore,
      boolean supportsSplitRewrite) {
    _rewriteEngine = MaterializedViewQueryRewriteEngineFactory.createDefault(propertyStore);
    _supportsSplitRewrite = supportsSplitRewrite;
  }

  @Override
  public boolean supportsSplitRewrite() {
    return _supportsSplitRewrite;
  }

  @Override
  public MaterializedViewContext compile(MaterializedViewCompileContext ctx) {
    MaterializedViewRewriteResult rewriteResult =
        _rewriteEngine.tryRewrite(ctx.getServerPinotQuery(), ctx.getRawTableName());
    if (rewriteResult == null || !rewriteResult.isHit()) {
      // Carry the rewrite result (which records candidates considered) so the response can still
      // surface evaluation metadata even when no MV applied.
      return MaterializedViewContext.fromRewriteResult(rewriteResult);
    }

    MaterializedViewRewritePlan plan = rewriteResult.getPlan();
    String mvTableNameWithType = plan.getMaterializedViewTableNameWithType();
    String mvRawTableName = TableNameBuilder.extractRawTableName(mvTableNameWithType);
    Schema mvSchema = ctx.getTableCache().getSchema(mvRawTableName);
    if (mvSchema == null) {
      LOGGER.warn("MV schema not found for {}; skipping rewrite", mvTableNameWithType);
      return MaterializedViewContext.fromRewriteResult(rewriteResult);
    }

    // Cold-start defense-in-depth: an MV with watermarkMs <= 0 has no committed coverage yet
    // (no APPEND has completed). The rewrite engine's resolvePlan already filters cold-start
    // candidates for split-spec MVs (which carry watermarkMs on the plan) — this guard re-checks
    // them here in case a future engine change loses that filter. ONLY applies to SPLIT_REWRITE
    // because FULL_REWRITE plans intentionally carry watermarkMs=0 (no boundary literal is
    // attached at execute time, so the watermark is irrelevant) and a blanket `<= 0` check would
    // wrongly drop every FULL_REWRITE attempt.
    if (plan.getExecMode() == ExecutionMode.SPLIT_REWRITE && plan.getWatermarkMs() <= 0) {
      LOGGER.debug("MV {} has watermarkMs={} <= 0; skipping SPLIT_REWRITE for request",
          mvTableNameWithType, plan.getWatermarkMs());
      return MaterializedViewContext.fromRewriteResult(rewriteResult);
    }

    if (plan.getExecMode() == ExecutionMode.SPLIT_REWRITE) {
      if (!_supportsSplitRewrite) {
        LOGGER.debug("Handler does not support SPLIT_REWRITE; skipping for MV {}", mvTableNameWithType);
        return MaterializedViewContext.fromRewriteResult(rewriteResult);
      }
      return MaterializedViewContext.forSplitRewrite(rewriteResult, plan.getMaterializedViewQuery(),
          mvTableNameWithType, mvRawTableName, mvSchema);
    }

    // FULL_REWRITE: caller swaps serverQuery/tableName/schema to point at the MV. The
    // pre-rewrite values are preserved in the returned context so ACL/quota/RLS authorize against
    // the original base table, not the MV that replaced it.
    return MaterializedViewContext.forFullRewrite(rewriteResult, ctx.getServerPinotQuery(),
        ctx.getTableNameWithType());
  }

  @Override
  public BrokerResponseNative executeSplit(MaterializedViewSplitExecutionContext ctx) throws Exception {
    MaterializedViewContext mvContext = ctx.getMaterializedViewContext();
    SplitRewriteContext splitRewriteContext = Preconditions.checkNotNull(mvContext.getSplitRewriteContext(),
        "MV split context must be set for split execution");
    MaterializedViewRewritePlan plan = Preconditions.checkNotNull(mvContext.getPlan(),
        "MV rewrite plan must be set for split execution");
    long boundaryTimeMs = plan.getWatermarkMs();

    MaterializedViewSplitSpec splitSpec = plan.getSplitSpec();
    Preconditions.checkNotNull(splitSpec,
        "MaterializedViewSplitSpec must be set when execMode == SPLIT_REWRITE for MV: %s",
        plan.getMaterializedViewTableNameWithType());

    // Both base and MV columns may use any DateTimeFieldSpec format (INT days, TIMESTAMP millis,
    // SIMPLE_DATE_FORMAT, etc.), so each boundary literal goes through its own
    // `*TimeFormat.fromMillisToFormat` to land in the column's native unit.  Without this, the
    // raw millis literal applied to an INT-days column would produce "Cannot convert value to
    // type: INT" at scan time.
    String baseTimeColumn = splitSpec.getSourceTimeColumn();
    String viewTimeColumn = splitSpec.getMaterializedViewTimeColumn();
    String sourceTimeFormat = splitSpec.getSourceTimeFormat();
    String viewTimeFormat = splitSpec.getMaterializedViewTimeFormat();
    Preconditions.checkState(sourceTimeFormat != null && !sourceTimeFormat.isEmpty(),
        "Internal error: sourceTimeFormat must be non-null/non-empty at SPLIT_REWRITE execution for MV: %s",
        plan.getMaterializedViewTableNameWithType());
    // Rolling-upgrade safety: definition znodes written by a pre-V2 controller persisted only
    // sourceTimeFormat.  When the broker upgrades ahead of the controller, an existing MV's
    // splitMaterializedViewTimeFormat is null.  Inferring it from sourceTimeFormat would
    // silently double-count rows when the two columns use different formats (e.g. base in
    // millis-epoch, MV in days-epoch — the day-valued column would always satisfy
    // `materializedViewTime < <millis-literal>`).  Refuse to dispatch and surface a clear
    // QueryException so the broker's outer try/catch falls back to the base-table path with
    // QUERY_REWRITE_EXCEPTIONS bumped; the operator's signal is that the MV must be re-created
    // under the upgraded controller.
    Preconditions.checkState(viewTimeFormat != null && !viewTimeFormat.isEmpty(),
        "Materialized view %s was created by a pre-V2 controller and is missing "
            + "splitMaterializedViewTimeFormat. Re-create the MV under the upgraded controller, "
            + "or query the base table directly. Broker will fall back to the base-table query path.",
        plan.getMaterializedViewTableNameWithType());
    String baseBoundaryLiteral =
        new DateTimeFormatSpec(sourceTimeFormat).fromMillisToFormat(boundaryTimeMs);
    String viewBoundaryLiteral =
        new DateTimeFormatSpec(viewTimeFormat).fromMillisToFormat(boundaryTimeMs);
    TimeBoundaryInfo baseBoundary = new TimeBoundaryInfo(baseTimeColumn, baseBoundaryLiteral);
    TimeBoundaryInfo viewBoundary = new TimeBoundaryInfo(viewTimeColumn, viewBoundaryLiteral);

    // --- Build base-side query with `ts >= boundary` filter ---
    // MV split requires servers to return intermediate results so the broker can merge DataTables
    // from both the base table and the MV correctly. LIMIT/OFFSET semantics: both sub-queries
    // (base + MV) inherit the user's original LIMIT and OFFSET unchanged. The final reduce uses
    // the original BrokerRequest (which retains those values), so OFFSET is correctly applied
    // during the merge phase, not at the sub-query level.
    PinotQuery basePinotQuery = ctx.getBaseServerPinotQuery().deepCopy();
    // CalciteSqlParser normally populates queryOptions; defend against tests / SDK paths that
    // construct a PinotQuery with null options to avoid an NPE that would surface as a generic
    // Exception in the broker's outer catch and silently bump QUERY_REWRITE_EXCEPTIONS.
    if (basePinotQuery.getQueryOptions() != null) {
      basePinotQuery.getQueryOptions().remove(QueryOptionKey.SERVER_RETURN_FINAL_RESULT);
      basePinotQuery.getQueryOptions().remove(QueryOptionKey.SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED);
    }
    // GREATER_THAN_OR_EQUAL (>=) covers ts == boundaryTimeMs since the MV materializes
    // ts < boundaryTimeMs (exclusive upper bound). Pairing >= on base with < on MV makes the
    // two halves of the timeline disjoint and exhaustive.
    attachFilter(basePinotQuery, baseBoundary, FilterKind.GREATER_THAN_OR_EQUAL);

    // --- Build MV-side query with `materializedViewTime < boundary` filter ---
    // Without this filter, when the broker observes a stale watermarkMs (during the
    // executor's non-atomic endSegmentReplace -> ZK publish window) rows in
    // [W_observed, W_actual) would be visible on both branches and double-counted.
    PinotQuery viewPinotQuery = splitRewriteContext.getMaterializedViewServerPinotQuery().deepCopy();
    if (viewPinotQuery.getQueryOptions() != null) {
      viewPinotQuery.getQueryOptions().remove(QueryOptionKey.SERVER_RETURN_FINAL_RESULT);
      viewPinotQuery.getQueryOptions().remove(QueryOptionKey.SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED);
    }
    attachFilter(viewPinotQuery, viewBoundary, FilterKind.LESS_THAN);

    LOGGER.info("MV split execution for request {}: baseTable={}, materializedViewTable={}, "
            + "boundaryTimeMs={}, baseColumn={}, viewColumn={}",
        ctx.getRequestId(), ctx.getBaseTableNameWithType(),
        splitRewriteContext.getMaterializedViewTableNameWithType(), boundaryTimeMs,
        baseTimeColumn, viewTimeColumn);

    // --- 4. Hand off to the broker for hybrid route prep + dual scatter-gather + reduce ---
    return ctx.getDispatcher().dispatch(ctx.getOriginalBrokerRequest(), basePinotQuery,
        ctx.getBaseRouteInfo(), ctx.getBaseSchema(), viewPinotQuery,
        splitRewriteContext.getMaterializedViewTableNameWithType(),
        splitRewriteContext.getMaterializedViewSchema(), ctx.getRemainingTimeMs());
  }

  @Override
  public void annotateResponse(BrokerResponseNative response, MaterializedViewContext mvContext) {
    // The response field is the operator's signal that an MV actually served the query. Gate on
    // the committed execution mode so a `fromRewriteResult` path (rewrite matched structurally
    // but the handler skipped the swap, e.g. SPLIT_REWRITE on a cold-start MV or an MV-schema
    // miss) does not produce a false-positive annotation. Without this guard, the broker would
    // report `materializedViewQueried=mv_orders_OFFLINE` even when the server query stayed
    // against the base table.
    if (mvContext == null || (!mvContext.isFullRewrite() && !mvContext.isSplitRewrite())) {
      return;
    }
    response.setMaterializedViewQueried(mvContext.getMaterializedViewQueriedName());
  }

  @Override
  public void invalidateBaseTable(String rawTableName) {
    _rewriteEngine.invalidateBaseTable(rawTableName);
  }

  @Override
  public void refreshTable(String rawTableName) {
    _rewriteEngine.refreshTable(rawTableName);
  }

  @Override
  public int getCacheEntryCount() {
    return _rewriteEngine.getCacheEntryCount();
  }

  /// Attaches a `col <op> value` filter to `pinotQuery`, AND-ing it onto any existing filter
  /// expression. Single helper used for both the base-side `>=` and the MV-side `<` boundary
  /// filters.
  @VisibleForTesting
  static void attachFilter(PinotQuery pinotQuery, TimeBoundaryInfo boundary, FilterKind op) {
    Expression timeFilter = RequestUtils.getFunctionExpression(op.name(),
        RequestUtils.getIdentifierExpression(boundary.getTimeColumn()),
        RequestUtils.getLiteralExpression(boundary.getTimeValue()));
    Expression existing = pinotQuery.getFilterExpression();
    pinotQuery.setFilterExpression(existing == null ? timeFilter
        : RequestUtils.getFunctionExpression(FilterKind.AND.name(), existing, timeFilter));
  }
}
