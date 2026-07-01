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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.creator.impl.ColumnarValueNormalizer;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.utils.PinotDataType;


/**
 * {@link SegmentPreIndexStatsContainer} that collects column statistics directly from {@link
 * ColumnReader} instances — one column at a time via the reader's sequential (rewind + next)
 * contract — rather than row by row. For a columnar source this keeps peak working memory to a single
 * column (or, for a batch-bounded reader, a single batch) instead of holding the whole row set, at
 * the cost of re-reading the source once per column. The values feed the same underlying collectors
 * as the row-based {@code SegmentPreIndexStatsCollectorImpl}.
 *
 * <p>Handles columns present in the source, new columns materialized from their defaults, and the
 * type coercion schema evolution requires (applied per value via {@link ColumnarValueNormalizer}).
 */
public class ColumnarSegmentPreIndexStatsContainer implements SegmentPreIndexStatsContainer {
  private final Map<String, ColumnStatistics> _columnStatisticsMap;
  private final int _totalDocCount;

  public ColumnarSegmentPreIndexStatsContainer(StatsCollectorConfig statsCollectorConfig,
      Map<String, ColumnReader> columnReaders) {
    _totalDocCount = resolveTotalDocs(columnReaders);

    Schema schema = statsCollectorConfig.getSchema();
    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    _columnStatisticsMap = Maps.newHashMapWithExpectedSize(fieldSpecs.size());
    if (_totalDocCount == 0) {
      buildEmptyStatistics(statsCollectorConfig, fieldSpecs);
    } else {
      collectColumnStatistics(statsCollectorConfig, schema, fieldSpecs, columnReaders);
    }
  }

  /** Validate that every reader agrees on the doc count and return it (throws if there are none). */
  private static int resolveTotalDocs(Map<String, ColumnReader> columnReaders) {
    int totalDocs = -1;
    for (ColumnReader columnReader : columnReaders.values()) {
      if (totalDocs < 0) {
        totalDocs = columnReader.getTotalDocs();
      } else {
        Preconditions.checkState(columnReader.getTotalDocs() == totalDocs,
            "ColumnReader totalDocs mismatch for column: %s. Expected: %s, got: %s", columnReader.getColumnName(),
            totalDocs, columnReader.getTotalDocs());
      }
    }
    Preconditions.checkState(totalDocs >= 0, "Total docs must not be negative, got: %s", totalDocs);
    return totalDocs;
  }

  /** Zero-doc segment: every non-virtual column gets empty statistics, mirroring the row-major path. */
  private void buildEmptyStatistics(StatsCollectorConfig statsCollectorConfig, Collection<FieldSpec> fieldSpecs) {
    for (FieldSpec fieldSpec : fieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        String columnName = fieldSpec.getName();
        PartitionFunction partitionFunction = statsCollectorConfig.getPartitionFunction(columnName);
        Set<Integer> partitions = partitionFunction != null ? Set.of() : null;
        _columnStatisticsMap.put(columnName, new EmptyColumnStatistics(fieldSpec, partitionFunction, partitions));
      }
    }
  }

  private void collectColumnStatistics(StatsCollectorConfig statsCollectorConfig, Schema schema,
      Collection<FieldSpec> fieldSpecs, Map<String, ColumnReader> columnReaders) {
    Map<String, FieldIndexConfigs> indexConfigsByCol =
        FieldIndexConfigsUtil.createIndexConfigsByColName(statsCollectorConfig.getTableConfig(), schema);
    for (FieldSpec fieldSpec : fieldSpecs) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }
      String columnName = fieldSpec.getName();
      AbstractColumnStatisticsCollector statsCollector =
          StatsCollectorUtil.createStatsCollector(columnName, fieldSpec, indexConfigsByCol.get(columnName),
              statsCollectorConfig);
      ColumnReader columnReader = columnReaders.get(columnName);
      Preconditions.checkState(columnReader != null, "Failed to find column reader for column: %s", columnName);
      collectColumn(columnName, fieldSpec, columnReader, statsCollector);
      statsCollector.seal();
      _columnStatisticsMap.put(columnName, statsCollector);
    }
  }

  /**
   * Consume one column sequentially (rewind + next) and feed each normalized value to its collector.
   *
   * <p>The column-major driver runs with no transform pipeline, so each value must be normalized here
   * the way the row-major {@code NullValueTransformer} + {@code DataTypeTransformer} would: substitute
   * the column default for nulls and coerce to the column's stored type (e.g. Boolean -> Integer for a
   * BOOLEAN column stored as INT, Timestamp -> Long for TIMESTAMP). Without it a non-segment source
   * (e.g. Arrow) feeds nulls / source-typed values into the typed collectors' {@code collect(Object)}
   * cast convention and NPEs / ClassCastExceptions. Shared with the index-write path via {@link
   * ColumnarValueNormalizer}; closes the null/type-handling gap in {@code buildColumnar()}
   * (apache/pinot#18629). Sequential (rewindable) consumption is the contract every columnar source
   * can satisfy cheaply, including lazy / streaming readers that cannot serve random docId access.
   */
  private static void collectColumn(String columnName, FieldSpec fieldSpec, ColumnReader columnReader,
      AbstractColumnStatisticsCollector statsCollector) {
    PinotDataType destDataType = PinotDataType.getPinotDataTypeForIngestion(fieldSpec);
    try {
      columnReader.rewind();
      // Typed fast path: when the reader can serve a single-value column as a primitive directly
      // (ColumnReader.isInt()/isLong()/...), read it with the type-specific accessor and feed the
      // matching AbstractColumnStatisticsCollector.collect(primitive) overload. This avoids the
      // Integer/Long/Float/Double box that next() -> collect(Object) incurs on every value, plus the
      // per-value normalize() allocation. Anything the reader cannot type directly — multi-value,
      // BIG_DECIMAL/STRING/BYTES, or a column needing coercion (BOOLEAN/TIMESTAMP) — falls through to
      // the shared, normalize()-based Object path below, which stays the single source of truth for
      // null and type handling.
      if (fieldSpec.isSingleValueField()
          && collectSingleValuePrimitive(columnName, fieldSpec, destDataType, columnReader, statsCollector)) {
        return;
      }
      while (columnReader.hasNext()) {
        statsCollector.collect(
            ColumnarValueNormalizer.normalize(columnName, fieldSpec, destDataType, columnReader.next()));
      }
    } catch (IOException e) {
      throw new RuntimeException("Caught exception collecting stats for column: " + columnName, e);
    }
  }

  /**
   * Typed, allocation-free stats collection for a single-value primitive column. Returns {@code true}
   * if it consumed the column (the reader served it as INT / LONG / FLOAT / DOUBLE directly), or
   * {@code false} — without advancing the reader — if the caller should fall back to the Object path.
   *
   * <p>A {@code null} value is collected as the column's default, pre-normalized once via {@link
   * ColumnarValueNormalizer} so it matches the value the Object path would collect (segment metadata
   * stays identical between the two paths). The reader's {@code isInt()} / {@code isLong()} / ...
   * capability check guards each typed accessor, so a column the reader cannot type natively (e.g.
   * BOOLEAN or TIMESTAMP read from a non-native vector) returns {@code false} here rather than risking
   * a wrong typed read.
   */
  private static boolean collectSingleValuePrimitive(String columnName, FieldSpec fieldSpec,
      PinotDataType destDataType, ColumnReader columnReader, AbstractColumnStatisticsCollector statsCollector)
      throws IOException {
    switch (fieldSpec.getDataType()) {
      case INT: {
        if (!columnReader.isInt()) {
          return false;
        }
        while (columnReader.hasNext()) {
          if (columnReader.isNextNull()) {
            collectNull(columnName, fieldSpec, destDataType, columnReader, statsCollector);
          } else {
            statsCollector.collect(columnReader.nextInt());
          }
        }
        return true;
      }
      case LONG: {
        if (!columnReader.isLong()) {
          return false;
        }
        while (columnReader.hasNext()) {
          if (columnReader.isNextNull()) {
            collectNull(columnName, fieldSpec, destDataType, columnReader, statsCollector);
          } else {
            statsCollector.collect(columnReader.nextLong());
          }
        }
        return true;
      }
      case FLOAT: {
        if (!columnReader.isFloat()) {
          return false;
        }
        while (columnReader.hasNext()) {
          if (columnReader.isNextNull()) {
            collectNull(columnName, fieldSpec, destDataType, columnReader, statsCollector);
          } else {
            statsCollector.collect(columnReader.nextFloat());
          }
        }
        return true;
      }
      case DOUBLE: {
        if (!columnReader.isDouble()) {
          return false;
        }
        while (columnReader.hasNext()) {
          if (columnReader.isNextNull()) {
            collectNull(columnName, fieldSpec, destDataType, columnReader, statsCollector);
          } else {
            statsCollector.collect(columnReader.nextDouble());
          }
        }
        return true;
      }
      default:
        return false;
    }
  }

  /**
   * Collect a null doc on the typed fast path by routing it through the same {@link
   * ColumnarValueNormalizer#normalize} call {@code collectColumn} uses, so this path records exactly
   * what the Object path would: a source that returns {@code null} at a null doc (e.g. Arrow) yields
   * the column default, and one that surfaces a stored sentinel (e.g. a Pinot-segment reader) yields
   * that sentinel — both identical to the Object path. Null docs are rare, so the box this incurs is
   * immaterial; the non-null hot path stays primitive.
   */
  private static void collectNull(String columnName, FieldSpec fieldSpec, PinotDataType destDataType,
      ColumnReader columnReader, AbstractColumnStatisticsCollector statsCollector)
      throws IOException {
    statsCollector.collect(
        ColumnarValueNormalizer.normalize(columnName, fieldSpec, destDataType, columnReader.next()));
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatisticsMap.get(column);
  }

  @Override
  public int getTotalDocCount() {
    return _totalDocCount;
  }
}
