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

import java.util.List;
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
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.validate.Validator;
import org.apache.pinot.spi.data.Schema;


/**
 * Wrapper for pinot internal info for a table.
 *
 * <p>This construct is used to connect a Pinot table to Apache Calcite's relational planner by providing a
 * {@link RelDataType} of the table to the planner.
 *
 * <h3>Lifecycle and caching</h3>
 * <p>A new {@code PinotTable} instance is created by {@link PinotCatalog#getTable(String)} on every
 * catalog lookup, which happens at most a few times per table per query during planning.
 * Therefore no additional caching is needed inside this class: each instance calls the statistics
 * provider at most once when Calcite calls {@link #getStatistic()}.
 *
 * <h3>Thread-safety</h3>
 * <p>Instances are created and used exclusively on the planner thread; no concurrent access occurs.
 */
public class PinotTable extends AbstractTable implements ScannableTable {
  private Schema _schema;
  private boolean _excludeVirtualColumns = false;
  private final String _tableName;
  private final PinotStatisticsProvider _statisticsProvider;

  public PinotTable(Schema schema) {
    this(schema, false);
  }

  /**
   * Constructor with option to exclude virtual columns.
   * This is typically used for NATURAL JOIN operations where virtual columns
   * should not participate in join condition matching.
   */
  public PinotTable(Schema schema, boolean excludeVirtualColumns) {
    this(schema, excludeVirtualColumns, null, NoOpStatisticsProvider.INSTANCE);
  }

  /**
   * Full constructor.
   *
   * @param schema               the Pinot table schema
   * @param excludeVirtualColumns whether to exclude virtual columns from the row type
   * @param tableName            the resolved logical table name passed to the statistics provider;
   *                             may be {@code null} when statistics are not needed
   * @param statisticsProvider   the provider that supplies row-count statistics to the planner
   */
  public PinotTable(Schema schema, boolean excludeVirtualColumns, @Nullable String tableName,
      PinotStatisticsProvider statisticsProvider) {
    _schema = schema;
    _excludeVirtualColumns = excludeVirtualColumns;
    _tableName = tableName;
    _statisticsProvider = statisticsProvider;
  }

  /**
   * Returns the Calcite {@link Statistic} for this table, exposing the row count to the planner
   * when reliable statistics are available.
   *
   * <p>The row count is surfaced only when the {@link TableStatistics#getRowCountConfidence()}
   * is {@link StatConfidence#EXACT} or {@link StatConfidence#ESTIMATED} and the row count is
   * non-negative. {@link StatConfidence#LOW} and {@link StatConfidence#UNKNOWN} are treated as
   * absent (returns {@link Statistics#UNKNOWN}) because systematically biased values would mislead
   * the cost-based optimizer.
   *
   * <p>Calcite contract: {@link Statistic#getRowCount()} may return {@code null} to signal
   * "unknown"; the planner then falls back to heuristic estimation as before CBO was introduced.
   */
  @Override
  public Statistic getStatistic() {
    if (_tableName == null) {
      return Statistics.UNKNOWN;
    }
    TableStatistics stats = _statisticsProvider.getTableStatistics(_tableName);
    if (stats == null) {
      return Statistics.UNKNOWN;
    }
    long rowCount = stats.getRowCount();
    StatConfidence confidence = stats.getRowCountConfidence();
    if (rowCount < 0 || confidence == StatConfidence.LOW || confidence == StatConfidence.UNKNOWN) {
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

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }
}
