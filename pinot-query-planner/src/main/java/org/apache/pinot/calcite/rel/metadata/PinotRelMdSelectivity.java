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
package org.apache.pinot.calcite.rel.metadata;

import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.query.catalog.PinotTable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;


/// Metadata handler that improves selectivity estimation for Pinot tables using
/// [PinotStatisticsProvider].
///
/// ### Handled cases
/// - **Time-range selectivity**: conjuncts that compare the primary millisecond time column
///   against literal values using `>`, `>=`, `<`, `<=`, `=`,
///   or BETWEEN (expressed as two conjuncts) are evaluated via
///   [PinotStatisticsProvider#estimateRowsInTimeRange].
/// - **NDV equality selectivity**: equality predicates on non-time columns with known NDV
///   (EXACT or ESTIMATED confidence) yield `1/ndv`.
/// - **Everything else**: falls back to [RelMdUtil#guessSelectivity].
///
/// ### Column-index remapping
/// When the parent [Filter] sits directly on a [TableScan] or on a trivial
/// [Project] (all projections are plain [RexInputRef] with no reordering), column
/// indices referenced in the predicate match scan columns directly. Deeper stacks fall back to the
/// default guess.
///
/// ### Thread-safety
/// Instances are stateless apart from the injected [PinotStatisticsProvider], which must
/// itself be thread-safe. This handler is safe for concurrent use once constructed.
public class PinotRelMdSelectivity implements MetadataHandler<BuiltInMetadata.Selectivity> {

  private static final double FALLBACK_RANGE_SELECTIVITY = 0.25;

  @Override
  public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
    return BuiltInMetadata.Selectivity.DEF;
  }

  // --------------------------------------------------------------------------
  // Handler entry-points (dispatched by Calcite via reflection)
  // --------------------------------------------------------------------------

  /// Selectivity for a [Filter] whose predicate may reference a Pinot time column or NDV
  /// statistics.
  ///
  /// Only predicates that can be traced directly to a Pinot scan (Filter-on-Scan or
  /// Filter-on-trivial-Project-on-Scan) are enhanced; all other shapes delegate to the default.
  public Double getSelectivity(Filter rel, RelMetadataQuery mq, @Nullable RexNode predicate) {
    // Calcite contract (see RelMdSelectivity#getSelectivity(Filter, ...)): when a predicate is
    // passed in, subtract the filter's own condition first — its selectivity is already reflected
    // in the filter's row count, and estimating it again would double-apply it. A null predicate
    // means "apply the filter's own condition".
    RexNode effectivePred = predicate != null
        ? RelMdUtil.minusPreds(rel.getCluster().getRexBuilder(), predicate, rel.getCondition())
        : rel.getCondition();

    // Resolve the scan and column-index mapping (null if the chain is too deep).
    ScanContext ctx = resolveScan(rel.getInput());
    if (ctx == null) {
      // Not a recognizable Pinot scan chain: delegate to the input like the default handler so
      // Calcite's input-specific handlers (Project/Aggregate/Union) still apply.
      return mq.getSelectivity(rel.getInput(), effectivePred);
    }
    return computeSelectivity(effectivePred, ctx, mq);
  }

  /// Selectivity for a bare [TableScan] — used when selectivity is requested directly
  /// on the scan (no enclosing Filter).
  public Double getSelectivity(TableScan rel, RelMetadataQuery mq, @Nullable RexNode predicate) {
    if (predicate == null) {
      return 1.0;
    }
    PinotTable pinotTable = rel.getTable().unwrap(PinotTable.class);
    if (pinotTable == null) {
      return RelMdUtil.guessSelectivity(predicate);
    }
    ScanContext ctx = new ScanContext(pinotTable, rel.getRowType().getFieldNames(), null);
    return computeSelectivity(predicate, ctx, mq);
  }

  // --------------------------------------------------------------------------
  // Core selectivity computation
  // --------------------------------------------------------------------------

  private double computeSelectivity(@Nullable RexNode predicate, ScanContext ctx,
      RelMetadataQuery mq) {
    if (predicate == null) {
      return 1.0;
    }

    PinotTable table = ctx._table;
    String tableName = table.getTableName();
    if (tableName == null) {
      return RelMdUtil.guessSelectivity(predicate);
    }

    PinotStatisticsProvider provider = table.getStatisticsProvider();
    TableStatistics tableStats = provider.getTableStatistics(tableName);
    double rowCount = (tableStats != null && tableStats.getRowCount() >= 0
        && tableStats.getRowCountConfidence() != StatConfidence.LOW
        && tableStats.getRowCountConfidence() != StatConfidence.UNKNOWN)
        ? (double) tableStats.getRowCount() : -1;

    String timeCol = table.getMillisTimeColumnName();

    List<RexNode> conjuncts = RelOptUtil.conjunctions(predicate);
    if (conjuncts.isEmpty()) {
      return 1.0;
    }

    // Collect time-range bounds and non-time conjuncts separately.
    long timeLo = Long.MIN_VALUE;
    long timeHi = Long.MAX_VALUE;
    boolean hasTimePredicate = false;
    double nonTimeSelectivity = 1.0;

    for (RexNode conjunct : conjuncts) {
      if (timeCol != null && rowCount > 0) {
        // Handle Calcite 1.40+ SEARCH(col, Sarg[...]) optimization.
        if (conjunct.getKind() == SqlKind.SEARCH) {
          SearchTimeBounds stb = extractSearchTimeBounds(conjunct, timeCol, ctx);
          if (stb != null) {
            hasTimePredicate = true;
            timeLo = Math.max(timeLo, stb._lo);
            timeHi = Math.min(timeHi, stb._hi);
            continue;
          }
        } else {
          TimeBound tb = extractTimeBound(conjunct, timeCol, ctx);
          if (tb != null) {
            hasTimePredicate = true;
            if (tb._isEquality) {
              // Equality: pin both bounds to the single millisecond value.
              timeLo = Math.max(timeLo, tb._millis);
              timeHi = Math.min(timeHi, tb._millis);
            } else if (tb._isLowerBound) {
              timeLo = Math.max(timeLo, tb._millis);
            } else {
              timeHi = Math.min(timeHi, tb._millis);
            }
            continue;
          }
        }
      }
      nonTimeSelectivity *= computeConjunctSelectivity(conjunct, ctx, tableName, provider, rowCount);
    }

    double timeSelectivity = 1.0;
    if (hasTimePredicate && rowCount > 0) {
      if (timeLo > timeHi) {
        // Impossible range — predicate is always false.
        return 0.0;
      }
      long lo = timeLo == Long.MIN_VALUE ? 0 : timeLo;
      // timeLo/timeHi are tracked as INCLUSIVE bounds; estimateRowsInTimeRange takes a half-open
      // [startMs, endMs) interval, so the inclusive upper bound must be converted with +1
      // (saturating to avoid overflow). Without this, equality predicates (lo == hi) would yield
      // an empty range and a selectivity of 0.
      long hiExclusive = timeHi >= Long.MAX_VALUE - 1 ? Long.MAX_VALUE : timeHi + 1;
      OptionalLong estimated = provider.estimateRowsInTimeRange(tableName, lo, hiExclusive);
      if (estimated.isPresent()) {
        timeSelectivity = Math.min(1.0, Math.max(0.0, (double) estimated.getAsLong() / rowCount));
      } else {
        timeSelectivity = FALLBACK_RANGE_SELECTIVITY;
      }
    }

    return Math.min(1.0, Math.max(0.0, timeSelectivity * nonTimeSelectivity));
  }

  // --------------------------------------------------------------------------
  // Per-conjunct selectivity
  // --------------------------------------------------------------------------

  private double computeConjunctSelectivity(RexNode conjunct, ScanContext ctx, String tableName,
      PinotStatisticsProvider provider, double rowCount) {
    if (conjunct.getKind() != SqlKind.EQUALS) {
      return RelMdUtil.guessSelectivity(conjunct);
    }
    // Equality on a non-time column: try NDV.
    String colName = extractColumnRef(conjunct, ctx);
    if (colName == null) {
      return RelMdUtil.guessSelectivity(conjunct);
    }
    ColumnStatistics colStats = provider.getColumnStatistics(tableName, colName);
    if (colStats == null) {
      return RelMdUtil.guessSelectivity(conjunct);
    }
    StatConfidence conf = colStats.getNdvConfidence();
    if (conf == StatConfidence.EXACT || conf == StatConfidence.ESTIMATED) {
      long ndv = colStats.getNdv();
      if (ndv >= 1) {
        return 1.0 / ndv;
      }
    }
    return RelMdUtil.guessSelectivity(conjunct);
  }

  // --------------------------------------------------------------------------
  // Time-bound extraction
  // --------------------------------------------------------------------------

  /// Tries to extract a time bound from a comparison conjunct. Returns `null` if this
  /// conjunct is not a recognized time comparison.
  ///
  /// Supported patterns (both orientations):
  /// - `timeCol > literal`  → lower bound (exclusive), stored as `literal + 1`
  /// - `timeCol >= literal` → lower bound (inclusive), stored as `literal`
  /// - `timeCol < literal`  → upper bound (exclusive), stored as `literal - 1`
  /// - `timeCol <= literal` → upper bound (inclusive), stored as `literal`
  /// - `timeCol = literal`  → both bounds, returned as lower bound (`hi` resolved
  ///   separately via the same call with flipped bound flag)
  @Nullable
  private TimeBound extractTimeBound(RexNode conjunct, String timeCol, ScanContext ctx) {
    SqlKind kind = conjunct.getKind();
    if (!(conjunct instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) conjunct;
    if (call.operands.size() != 2) {
      return null;
    }
    RexNode left = call.operands.get(0);
    RexNode right = call.operands.get(1);

    // Normalise: if the literal is on the left, flip the operator.
    SqlKind effectiveKind = kind;
    RexNode colOperand = left;
    RexNode litOperand = right;
    if (left instanceof RexLiteral && !(right instanceof RexLiteral)) {
      colOperand = right;
      litOperand = left;
      effectiveKind = flipComparison(kind);
    } else if (!(left instanceof RexInputRef) && !(right instanceof RexLiteral)) {
      return null;
    }

    // The column operand must refer to the time column.
    if (!(colOperand instanceof RexInputRef)) {
      return null;
    }
    String refColName = resolveColName((RexInputRef) colOperand, ctx);
    if (!timeCol.equals(refColName)) {
      return null;
    }

    if (!(litOperand instanceof RexLiteral)) {
      return null;
    }
    long millis = extractMillis((RexLiteral) litOperand);
    if (millis == Long.MIN_VALUE) {
      return null;
    }

    switch (effectiveKind) {
      case GREATER_THAN:
        return new TimeBound(millis + 1, true);  // exclusive lower → millis+1 inclusive
      case GREATER_THAN_OR_EQUAL:
        return new TimeBound(millis, true);
      case LESS_THAN:
        return new TimeBound(millis - 1, false); // exclusive upper → millis-1 inclusive
      case LESS_THAN_OR_EQUAL:
        return new TimeBound(millis, false);
      case EQUALS:
        // Equality pins both bounds; the caller clamps timeLo and timeHi to this value.
        return new TimeBound(millis, true, true /* isEquality */);
      default:
        return null;
    }
  }

  /// Tries to extract a single consolidated time range from a Calcite `SEARCH(col, Sarg)`
  /// conjunct. This handles the case where Calcite rewrites `col >= A AND col < B` into a
  /// single `SEARCH` call.
  ///
  /// Only single-range Sargs over the time column are handled; multi-range Sargs (OR of ranges)
  /// fall back to the default guess.
  ///
  /// @return combined time bounds, or `null` if not applicable
  @Nullable
  @SuppressWarnings("unchecked")
  private SearchTimeBounds extractSearchTimeBounds(RexNode conjunct, String timeCol,
      ScanContext ctx) {
    if (!(conjunct instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) conjunct;
    if (call.operands.size() != 2) {
      return null;
    }
    RexNode colOperand = call.operands.get(0);
    RexNode sargOperand = call.operands.get(1);

    if (!(colOperand instanceof RexInputRef) || !(sargOperand instanceof RexLiteral)) {
      return null;
    }
    String refColName = resolveColName((RexInputRef) colOperand, ctx);
    if (!timeCol.equals(refColName)) {
      return null;
    }

    Sarg sarg = ((RexLiteral) sargOperand).getValueAs(Sarg.class);
    if (sarg == null) {
      return null;
    }

    Set<Range<Comparable>> ranges = (Set<Range<Comparable>>) (Set<?>) sarg.rangeSet.asRanges();
    // Only handle a single contiguous range.
    if (ranges.size() != 1) {
      return null;
    }
    Range<Comparable> range = ranges.iterator().next();

    long lo = range.hasLowerBound()
        ? toInclusiveMillis(range.lowerEndpoint(), range.lowerBoundType(), true) : Long.MIN_VALUE;
    long hi = range.hasUpperBound()
        ? toInclusiveMillis(range.upperEndpoint(), range.upperBoundType(), false) : Long.MAX_VALUE;

    if (lo == Long.MIN_VALUE && hi == Long.MAX_VALUE) {
      return null; // unbounded — no useful info
    }
    return new SearchTimeBounds(lo, hi);
  }

  /// Converts a range endpoint to an INCLUSIVE epoch-millisecond bound, adjusting OPEN (exclusive)
  /// endpoints by one millisecond in the appropriate direction.
  ///
  /// @param endpoint        the comparable endpoint value (expected to be a [BigDecimal] or Number)
  /// @param boundType       Guava [com.google.common.collect.BoundType]
  /// @param isLowerEndpoint `true` when converting the lower endpoint of the range
  /// @return inclusive millisecond bound, or [Long#MIN_VALUE] if not convertible
  private static long toInclusiveMillis(Comparable<?> endpoint,
      com.google.common.collect.BoundType boundType, boolean isLowerEndpoint) {
    long millis;
    if (endpoint instanceof BigDecimal) {
      millis = ((BigDecimal) endpoint).longValue();
    } else if (endpoint instanceof Number) {
      millis = ((Number) endpoint).longValue();
    } else {
      return Long.MIN_VALUE;
    }
    if (boundType == com.google.common.collect.BoundType.OPEN) {
      // OPEN lower bound x means values > x, i.e. inclusive lower bound x+1.
      // OPEN upper bound x means values < x, i.e. inclusive upper bound x-1.
      millis = isLowerEndpoint ? millis + 1 : millis - 1;
    }
    return millis;
  }

  /// Flips a comparison operator (for when the literal is on the left).
  private static SqlKind flipComparison(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }

  /// Extracts the millisecond value from a [RexLiteral]. Supports BIGINT/INTEGER numerics
  /// and TIMESTAMP literals (stored as epoch-ms in Calcite). Returns [Long#MIN_VALUE] if
  /// the literal cannot be interpreted as epoch milliseconds.
  private static long extractMillis(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case BIGINT:
      case INTEGER:
      case DECIMAL: {
        Number n = (Number) literal.getValue();
        return n != null ? n.longValue() : Long.MIN_VALUE;
      }
      case TIMESTAMP: {
        // Calcite stores TIMESTAMP literals as Calendar or NlsString; the value2 is epoch-ms.
        Number n = (Number) literal.getValue2();
        return n != null ? n.longValue() : Long.MIN_VALUE;
      }
      default:
        return Long.MIN_VALUE;
    }
  }

  // --------------------------------------------------------------------------
  // Column-name resolution
  // --------------------------------------------------------------------------

  /// For an equality call, returns the referenced column name if one operand is a scan column
  /// reference and the other is a literal; otherwise returns `null`.
  @Nullable
  private String extractColumnRef(RexNode conjunct, ScanContext ctx) {
    if (!(conjunct instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) conjunct;
    if (call.operands.size() != 2) {
      return null;
    }
    RexNode left = call.operands.get(0);
    RexNode right = call.operands.get(1);

    RexInputRef ref = null;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      ref = (RexInputRef) left;
    } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
      ref = (RexInputRef) right;
    }
    if (ref == null) {
      return null;
    }
    return resolveColName(ref, ctx);
  }

  /// Resolves a [RexInputRef] index to the scan's column name, applying any trivial project
  /// remapping stored in the [ScanContext].
  ///
  /// Column indices are resolved against [ScanContext#_scanFieldNames], which reflects
  /// the ordered field list of the underlying [TableScan]'s `RelDataType` — matching
  /// exactly how Calcite assigns [RexInputRef] indices.
  ///
  /// @return column name, or `null` if the index cannot be resolved
  @Nullable
  private String resolveColName(RexInputRef ref, ScanContext ctx) {
    int scanIdx = ref.getIndex();
    if (ctx._projectMapping != null) {
      if (scanIdx < 0 || scanIdx >= ctx._projectMapping.size()) {
        return null;
      }
      scanIdx = ctx._projectMapping.get(scanIdx);
      if (scanIdx < 0) {
        return null;
      }
    }
    List<String> fieldNames = ctx._scanFieldNames;
    if (scanIdx < 0 || scanIdx >= fieldNames.size()) {
      return null;
    }
    return fieldNames.get(scanIdx);
  }

  // --------------------------------------------------------------------------
  // Scan-context resolution
  // --------------------------------------------------------------------------

  /// Walks from `input` downward through trivial [Project]s (all projections are plain
  /// [RexInputRef]) to find the underlying [TableScan].
  ///
  /// @return scan context, or `null` if the input is not a Pinot scan or the projection
  ///         is non-trivial
  @Nullable
  private ScanContext resolveScan(RelNode input) {
    if (input instanceof TableScan) {
      PinotTable pinotTable = input.getTable().unwrap(PinotTable.class);
      if (pinotTable == null) {
        return null;
      }
      // Use the scan's ordered field list — these are the column names in index order.
      List<String> fieldNames = input.getRowType().getFieldNames();
      return new ScanContext(pinotTable, fieldNames, null);
    }
    if (input instanceof Project) {
      Project project = (Project) input;
      List<RexNode> projects = project.getProjects();
      // Accept only trivial identity-style projections (each expression is a RexInputRef).
      int[] mapping = new int[projects.size()];
      for (int i = 0; i < projects.size(); i++) {
        RexNode expr = projects.get(i);
        if (!(expr instanceof RexInputRef)) {
          return null; // non-trivial expression — bail out
        }
        mapping[i] = ((RexInputRef) expr).getIndex();
      }
      ScanContext childCtx = resolveScan(project.getInput());
      if (childCtx == null) {
        return null;
      }
      // Compose mappings: outer mapping (i → mapping[i]) applied on top of inner.
      if (childCtx._projectMapping == null) {
        // Direct mapping to scan fields.
        List<Integer> mappingList = new java.util.ArrayList<>(mapping.length);
        for (int idx : mapping) {
          mappingList.add(idx);
        }
        return new ScanContext(childCtx._table, childCtx._scanFieldNames, mappingList);
      }
      // Compose two levels of indirection.
      List<Integer> composed = new java.util.ArrayList<>(mapping.length);
      for (int idx : mapping) {
        if (idx < 0 || idx >= childCtx._projectMapping.size()) {
          return null;
        }
        composed.add(childCtx._projectMapping.get(idx));
      }
      return new ScanContext(childCtx._table, childCtx._scanFieldNames, composed);
    }
    // Any other rel node (Filter, Join, Aggregate, etc.): bail to default.
    return null;
  }

  // --------------------------------------------------------------------------
  // Internal data classes
  // --------------------------------------------------------------------------

  /// Snapshot of scan context: the underlying [PinotTable], the ordered field names from the
  /// scan's row type, and an optional index remapping that translates predicate input-ref indices
  /// to scan-field indices.
  ///
  /// When `_projectMapping` is `null`, no remapping is needed (predicate refs go
  /// directly to scan fields).
  private static final class ScanContext {
    final PinotTable _table;
    /// Field names of the underlying TableScan's row type, in order.
    final List<String> _scanFieldNames;
    @Nullable
    final List<Integer> _projectMapping;

    ScanContext(PinotTable table, List<String> scanFieldNames,
        @Nullable List<Integer> projectMapping) {
      _table = table;
      _scanFieldNames = scanFieldNames;
      _projectMapping = projectMapping;
    }
  }

  /// Combined lower and upper time bounds extracted from a single SEARCH/Sarg conjunct.
  private static final class SearchTimeBounds {
    /// Inclusive lower epoch-millisecond bound; [Long#MIN_VALUE] if unbounded.
    final long _lo;
    /// Inclusive upper epoch-millisecond bound; [Long#MAX_VALUE] if unbounded.
    final long _hi;

    SearchTimeBounds(long lo, long hi) {
      _lo = lo;
      _hi = hi;
    }
  }

  /// A single time bound extracted from a comparison conjunct.
  ///
  /// @param _millis      epoch-millisecond value of the bound
  /// @param _isLowerBound `true` for a lower (start) bound; `false` for an upper (end)
  /// @param _isEquality  `true` if this bound came from an `=` predicate and both
  ///                     lower AND upper should be set to `_millis`
  private static final class TimeBound {
    final long _millis;
    final boolean _isLowerBound;
    final boolean _isEquality;

    TimeBound(long millis, boolean isLowerBound) {
      this(millis, isLowerBound, false);
    }

    TimeBound(long millis, boolean isLowerBound, boolean isEquality) {
      _millis = millis;
      _isLowerBound = isLowerBound;
      _isEquality = isEquality;
    }
  }
}
