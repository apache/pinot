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
package org.apache.pinot.query.catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.pinot.query.planner.spi.stats.NoOpStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.validate.Validator;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Wrapper for pinot internal info for a table.
///
/// This construct is used to connect a Pinot table to Apache Calcite's relational planner by providing a
/// [RelDataType] of the table to the planner.
///
/// ### Lifecycle and caching
/// A new `PinotTable` instance is created by [PinotCatalog#getTable(String)] on every catalog lookup,
/// which happens at most a few times per table per query during planning. Therefore no additional
/// caching is needed inside this class: each instance calls the statistics provider at most once when
/// Calcite calls [#getStatistic()].
///
/// ### Thread-safety
/// Instances are created and used exclusively on the planner thread; no concurrent access occurs.
public class PinotTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTable.class);

  private Schema _schema;
  private boolean _excludeVirtualColumns = false;
  private final String _tableName;
  private final PinotStatisticsProvider _statisticsProvider;
  @Nullable
  private final String _timeColumnName;
  /// Memoized result of {@link #getStatistic()}; Calcite may call it many times per planning.
  @Nullable
  private Statistic _statistic;
  /// Memoized result of [#getTableStatistics()]. A separate resolved flag is needed because `null`
  /// is a legitimate answer meaning "this table has no statistics".
  @Nullable
  private TableStatistics _tableStatistics;
  private boolean _tableStatisticsResolved;
  /// Memoized result of [#getMillisTimeColumnName()]; `null` is a valid answer, hence the flag.
  @Nullable
  private String _millisTimeColumn;
  private boolean _millisTimeColumnResolved;
  /// Memoized time-range estimates, keyed by `[startMs, endMs]`.
  private final Map<List<Long>, OptionalLong> _timeRangeEstimates = new HashMap<>();

  public PinotTable(Schema schema) {
    this(schema, false);
  }

  /// Constructor with option to exclude virtual columns.
  /// This is typically used for NATURAL JOIN operations where virtual columns
  /// should not participate in join condition matching.
  public PinotTable(Schema schema, boolean excludeVirtualColumns) {
    this(schema, excludeVirtualColumns, null, NoOpStatisticsProvider.INSTANCE, null);
  }

  /// Constructor without a time column; segment time boundaries cannot be used for selectivity.
  public PinotTable(Schema schema, boolean excludeVirtualColumns, @Nullable String tableName,
      PinotStatisticsProvider statisticsProvider) {
    this(schema, excludeVirtualColumns, tableName, statisticsProvider, null);
  }

  /// Full constructor.
  ///
  /// @param schema               the Pinot table schema
  /// @param excludeVirtualColumns whether to exclude virtual columns from the row type
  /// @param tableName            the resolved logical table name passed to the statistics provider;
  /// may be {@code null} when statistics are not needed
  /// @param statisticsProvider   the provider that supplies row-count statistics to the planner
  /// @param timeColumnName       the table's primary time column (from the table config validation
  /// config) that segment time boundaries are organized by; may be
  /// {@code null} when unknown
  public PinotTable(Schema schema, boolean excludeVirtualColumns, @Nullable String tableName,
      PinotStatisticsProvider statisticsProvider, @Nullable String timeColumnName) {
    _schema = schema;
    _excludeVirtualColumns = excludeVirtualColumns;
    _tableName = tableName;
    _statisticsProvider = statisticsProvider;
    _timeColumnName = timeColumnName;
  }

  /// Returns the Calcite {@link Statistic} for this table, exposing the row count to the planner
  /// when reliable statistics are available.
  ///
  /// <p>The row count is surfaced only when the {@link TableStatistics#getRowCountConfidence()}
  /// is {@link StatConfidence#EXACT} or {@link StatConfidence#ESTIMATED} and the row count is
  /// non-negative. {@link StatConfidence#LOW} and {@link StatConfidence#UNKNOWN} are treated as
  /// absent (returns {@link Statistics#UNKNOWN}) because systematically biased values would mislead
  /// the cost-based optimizer.
  ///
  /// <p>Calcite contract: {@link Statistic#getRowCount()} may return {@code null} to signal
  /// "unknown"; the planner then falls back to heuristic estimation as before CBO was introduced.
  @Override
  public Statistic getStatistic() {
    Statistic statistic = _statistic;
    if (statistic == null) {
      statistic = computeStatistic();
      _statistic = statistic;
    }
    return statistic;
  }

  /// The row count if it is trustworthy enough to cost a plan with, or -1.
  ///
  /// Single source of truth on purpose: this gate used to be applied independently by
  /// [#getStatistic()] and by the selectivity handler, so the numerator and denominator of one
  /// estimate could disagree. It is an allow-list rather than a deny-list because
  /// [StatConfidence] is append-only -- a future low-trust tier must default to being rejected,
  /// not silently trusted.
  public long getUsableRowCount() {
    TableStatistics stats = getTableStatistics();
    if (stats == null) {
      return -1;
    }
    long rowCount = stats.getRowCount();
    return rowCount >= 0 && stats.getRowCountConfidence().isUsableForCosting() ? rowCount : -1;
  }

  /// Returns this table's statistics, fetched at most once for the life of this instance.
  ///
  /// The selectivity handler asks for these on every Filter it estimates, and Hep/Volcano discard
  /// the RelMetadataQuery cache after every transformation -- so without memoizing, one compile
  /// issues a store read per rule application. On the SQLite store each of those borrows one of a
  /// handful of pooled connections with a bounded wait, which under concurrent planning turns into
  /// timeouts and, because a failed read silently degrades to a heuristic, into the same query
  /// getting different plans depending on pool contention.
  @Nullable
  public TableStatistics getTableStatistics() {
    if (_tableName == null) {
      return null;
    }
    if (!_tableStatisticsResolved) {
      _tableStatistics = _statisticsProvider.getTableStatistics(_tableName);
      _tableStatisticsResolved = true;
    }
    return _tableStatistics;
  }

  /// Returns the estimated number of rows in `[startMs, endMs)`, memoized per distinct range.
  ///
  /// A plan asks for the same range repeatedly as rules fire; see [#getTableStatistics()] for why
  /// repeating the underlying store read is costly.
  public OptionalLong estimateRowsInTimeRange(long startMs, long endMs) {
    if (_tableName == null) {
      return OptionalLong.empty();
    }
    return _timeRangeEstimates.computeIfAbsent(List.of(startMs, endMs),
        k -> _statisticsProvider.estimateRowsInTimeRange(_tableName, startMs, endMs));
  }

  private Statistic computeStatistic() {
    // Through the memo, never the provider directly. The store is written continuously by the
    // Helix cluster-change threads that collect segment metadata, so two independent reads of the
    // same table can disagree -- and Calcite would then take the scan cardinality from one
    // snapshot while the selectivity handler divides by the other, making the same query plan
    // differently from one compile to the next.
    long rowCount = getUsableRowCount();
    if (rowCount < 0) {
      return Statistics.UNKNOWN;
    }
    return Statistics.of((double) rowCount, List.of());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    TypeFactory typeFactory;
    if (relDataTypeFactory instanceof TypeFactory) {
      typeFactory = (TypeFactory) relDataTypeFactory;
    } else { // this can happen when using Frameworks.withPrepare, which wraps our factory in a JavaTypeFactoryImpl
      typeFactory = TypeFactory.INSTANCE;
    }

    if (_excludeVirtualColumns) {
      return typeFactory.createRelDataTypeFromSchema(_schema, Validator::isVirtualColumn);
    } else {
      return typeFactory.createRelDataTypeFromSchema(_schema);
    }
  }

  /// Returns the Pinot schema backing this table.
  public Schema getSchema() {
    return _schema;
  }

  /// Returns the logical table name passed to the statistics provider, or {@code null} when
  /// statistics are not needed.
  @Nullable
  public String getTableName() {
    return _tableName;
  }

  /// Returns the statistics provider bound to this table.
  public PinotStatisticsProvider getStatisticsProvider() {
    return _statisticsProvider;
  }

  /// Returns the name of this table's PRIMARY time column (the one segment time boundaries are
  /// organized by, per the table config) if and only if it stores values in epoch milliseconds;
  /// {@code null} otherwise.
  ///
  /// Time-range selectivity estimates compare predicate literals against segment time boundaries,
  /// which are derived from the primary time column — so only that column qualifies, and only when
  /// its values already ARE epoch milliseconds. Any other encoding would need a conversion that is
  /// not implemented yet, and is reported as absent so the estimate falls back to a heuristic
  /// rather than comparing a day number against an instant.
  @Nullable
  public String getMillisTimeColumnName() {
    if (!_millisTimeColumnResolved) {
      _millisTimeColumn = resolveMillisTimeColumn();
      _millisTimeColumnResolved = true;
    }
    return _millisTimeColumn;
  }

  @Nullable
  private String resolveMillisTimeColumn() {
    if (_timeColumnName == null) {
      return null;
    }
    DateTimeFieldSpec spec = _schema.getSpecForTimeColumn(_timeColumnName);
    if (spec == null) {
      return null;
    }
    DateTimeFormatSpec formatSpec = spec.getFormatSpec();
    // Short-circuit SIMPLE_DATE_FORMAT before attempting a conversion. Such a column is never
    // epoch millis, and asking the spec to parse "1" against a pattern like yyyyMMdd throws --
    // which, on the hot metadata path, means constructing and discarding an exception for every
    // selectivity estimate on a very common Pinot time-column format.
    if (formatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT) {
      return null;
    }
    // Ask the format spec to convert, rather than trusting getColumnUnit(): round-tripping 1 also
    // rejects a scaled epoch such as `10:MILLISECONDS:EPOCH`, whose values are millis/10.
    try {
      if (formatSpec.fromFormatToMillis(1L) != 1L) {
        return null;
      }
    } catch (RuntimeException e) {
      // A format that cannot convert a plain number is not epoch millis either. Logged because a
      // misconfigured time column otherwise disables time-range selectivity with no diagnostic.
      LOGGER.debug("Time column {} (format {}) is not epoch millis; time-range selectivity "
          + "disabled for this table", _timeColumnName, formatSpec, e);
      return null;
    }
    return _timeColumnName;
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }
}
