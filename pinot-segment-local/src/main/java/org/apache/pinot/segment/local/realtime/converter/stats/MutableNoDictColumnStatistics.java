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
package org.apache.pinot.segment.local.realtime.converter.stats;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;

import static org.apache.pinot.segment.spi.Constants.UNKNOWN_CARDINALITY;


public class MutableNoDictColumnStatistics implements ColumnStatistics, CLPStatsProvider {
  protected final DataSourceMetadata _dataSourceMetadata;
  protected final FieldSpec _fieldSpec;
  @Nullable
  protected final int[] _sortedDocIds;
  protected final boolean _isSortedColumn;
  protected final MutableForwardIndex _forwardIndex;

  // Lazily computed because it may require a full scan of the forward index, and it is queried multiple times per
  // column during segment creation. Left unsynchronized: an instance describes a single column and is reached only
  // through the per-column stats map, so it is confined to whichever thread creates that column. Even if that ever
  // changes, the segment no longer accepts documents by the time stats are collected, so a race can only recompute
  // the same value.
  private Boolean _sorted;

  // Lazily-computed min/max for columns whose mutable segment does not track them (ingestion-aggregated metric
  // columns). Populated on first access by scanning the sealed forward index; see computeMinMaxIfNeeded().
  private boolean _minMaxComputed;
  @Nullable
  private Comparable<?> _computedMinValue;
  @Nullable
  private Comparable<?> _computedMaxValue;
  // Sortedness observed by that same scan, since it walks the same docs in the same order computeSorted() would.
  // Null when no scan ran (min/max were tracked, or the type is not recovered), in which case computeSorted() scans.
  @Nullable
  private Boolean _scanSorted;

  public MutableNoDictColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIds, boolean isSortedColumn) {
    _dataSourceMetadata = dataSource.getDataSourceMetadata();
    _fieldSpec = _dataSourceMetadata.getFieldSpec();
    Preconditions.checkState(_dataSourceMetadata.getNumDocs() > 0,
        "Use EmptyColumnStatistics for empty column: %s", _fieldSpec.getName());
    _sortedDocIds = sortedDocIds;
    _isSortedColumn = isSortedColumn;
    _forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();
    Preconditions.checkState(_forwardIndex != null, "Failed to find forward index for column: %s",
        _fieldSpec.getName());
  }

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public int getTotalDocs() {
    return _dataSourceMetadata.getNumDocs();
  }

  @Override
  public Comparable<?> getMinValue() {
    Comparable<?> minValue = (Comparable<?>) _dataSourceMetadata.getMinValue();
    if (minValue != null) {
      return minValue;
    }
    computeMinMaxIfNeeded();
    return _computedMinValue;
  }

  @Override
  public Comparable<?> getMaxValue() {
    Comparable<?> maxValue = (Comparable<?>) _dataSourceMetadata.getMaxValue();
    if (maxValue != null) {
      return maxValue;
    }
    computeMinMaxIfNeeded();
    return _computedMaxValue;
  }

  /// Computes min/max by scanning the sealed forward index once, caching the result. Only invoked when the mutable
  /// segment reports null min/max, which happens for ingestion-aggregated metric columns: their values mutate in
  /// place during consumption, so `MutableSegmentImpl` deliberately skips min/max tracking for them. Without a value
  /// domain the BitSliced range index creator cannot subtract the min for INT/LONG columns, so we recover it here.
  ///
  /// This is the same pass {@link #isSorted()} already performs at seal time for these columns, not an extra one: it
  /// walks the docs in that method's order and records sortedness alongside min/max, and {@link #computeSorted()}
  /// reuses the result. So a segment commit still reads such a column exactly once.
  ///
  /// Scoped to single-value INT/LONG columns because those are the only types whose BitSliced range index reads
  /// min/max: FLOAT/DOUBLE use the full floating-point ordinal domain, and other stored types do not support the
  /// BitSliced range index. For every other case min/max remain null (unchanged behavior), so aggregated
  /// FLOAT/DOUBLE and sketch columns are never scanned here.
  ///
  /// Not thread-safe: like the rest of this class it is only exercised on the single-threaded segment-seal path.
  private void computeMinMaxIfNeeded() {
    if (_minMaxComputed) {
      return;
    }
    int numDocs = _dataSourceMetadata.getNumDocs();
    if (isSingleValue() && numDocs > 0) {
      switch (getStoredType()) {
        case INT: {
          int min = _forwardIndex.getInt(docId(0));
          int max = min;
          int prev = min;
          boolean sorted = true;
          for (int i = 1; i < numDocs; i++) {
            int curr = _forwardIndex.getInt(docId(i));
            min = Math.min(min, curr);
            max = Math.max(max, curr);
            sorted &= curr >= prev;
            prev = curr;
          }
          _computedMinValue = min;
          _computedMaxValue = max;
          _scanSorted = sorted;
          break;
        }
        case LONG: {
          long min = _forwardIndex.getLong(docId(0));
          long max = min;
          long prev = min;
          boolean sorted = true;
          for (int i = 1; i < numDocs; i++) {
            long curr = _forwardIndex.getLong(docId(i));
            min = Math.min(min, curr);
            max = Math.max(max, curr);
            sorted &= curr >= prev;
            prev = curr;
          }
          _computedMinValue = min;
          _computedMaxValue = max;
          _scanSorted = sorted;
          break;
        }
        default:
          // Other stored types either do not need min/max for their range index (FLOAT/DOUBLE) or do not support a
          // BitSliced range index at all: leave min/max null (unchanged behavior).
          break;
      }
    }
    // Set last so a re-entrant call cannot observe the flag as computed while the values are still being populated.
    _minMaxComputed = true;
  }

  /// Maps an iteration position to the docId to read, mirroring the order {@link #computeSorted()} walks so that a
  /// single pass can answer both the value domain and sortedness.
  private int docId(int index) {
    return _sortedDocIds != null ? _sortedDocIds[index] : index;
  }

  @Nullable
  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return UNKNOWN_CARDINALITY;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _forwardIndex.getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLongestElement() {
    return _forwardIndex.getLengthOfLongestElement();
  }

  @Override
  public boolean isAscii() {
    return _forwardIndex.isAscii();
  }

  @Override
  public boolean isSorted() {
    if (_sorted == null) {
      _sorted = computeSorted();
    }
    return _sorted;
  }

  private boolean computeSorted() {
    // Sorted column is guaranteed to be sorted by construction — no scan needed
    if (_isSortedColumn) {
      return true;
    }

    // Multi-valued column cannot be sorted
    if (!isSingleValue()) {
      return false;
    }

    // A single distinct value is always sorted — no scan needed. Min and max are tracked per raw value during
    // ingestion, but are left null when aggregated metrics are enabled; for those this call recovers them by
    // scanning, which also settles sortedness.
    Comparable<?> minValue = getMinValue();
    if (_scanSorted != null) {
      // Min/max were untracked and recovered by computeMinMaxIfNeeded() above. That scan walked the same docs in the
      // same order this method would, so it already answered sortedness: reuse it rather than scanning twice.
      return _scanSorted;
    }
    if (minValue != null && minValue.equals(getMaxValue())) {
      return true;
    }

    int numDocs = _dataSourceMetadata.getNumDocs();

    // Verify that values are non-decreasing when iterated in the given order. The BYTES path uses
    // ByteArray.compare (unsigned byte-wise lexicographic), which is identical to UuidUtils.compare's unsigned
    // 64-bit-word ordering on canonical 16-byte big-endian UUIDs, so a single comparator handles both.
    DataType storedType = getStoredType();
    if (_sortedDocIds != null) {
      switch (storedType) {
        case INT: {
          int prev = _forwardIndex.getInt(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            int curr = _forwardIndex.getInt(_sortedDocIds[i]);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case LONG: {
          long prev = _forwardIndex.getLong(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            long curr = _forwardIndex.getLong(_sortedDocIds[i]);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case FLOAT: {
          float prev = _forwardIndex.getFloat(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            float curr = _forwardIndex.getFloat(_sortedDocIds[i]);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case DOUBLE: {
          double prev = _forwardIndex.getDouble(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            double curr = _forwardIndex.getDouble(_sortedDocIds[i]);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case BIG_DECIMAL: {
          BigDecimal prev = _forwardIndex.getBigDecimal(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            BigDecimal curr = _forwardIndex.getBigDecimal(_sortedDocIds[i]);
            if (curr.compareTo(prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case STRING: {
          String prev = _forwardIndex.getString(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            String curr = _forwardIndex.getString(_sortedDocIds[i]);
            if (curr.compareTo(prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case BYTES: {
          byte[] prev = _forwardIndex.getBytes(_sortedDocIds[0]);
          for (int i = 1; i < numDocs; i++) {
            byte[] curr = _forwardIndex.getBytes(_sortedDocIds[i]);
            if (ByteArray.compare(curr, prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        default:
          throw new IllegalStateException("Unsupported stored type: " + storedType);
      }
    } else {
      switch (storedType) {
        case INT: {
          int prev = _forwardIndex.getInt(0);
          for (int i = 1; i < numDocs; i++) {
            int curr = _forwardIndex.getInt(i);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case LONG: {
          long prev = _forwardIndex.getLong(0);
          for (int i = 1; i < numDocs; i++) {
            long curr = _forwardIndex.getLong(i);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case FLOAT: {
          float prev = _forwardIndex.getFloat(0);
          for (int i = 1; i < numDocs; i++) {
            float curr = _forwardIndex.getFloat(i);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case DOUBLE: {
          double prev = _forwardIndex.getDouble(0);
          for (int i = 1; i < numDocs; i++) {
            double curr = _forwardIndex.getDouble(i);
            if (curr < prev) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case BIG_DECIMAL: {
          BigDecimal prev = _forwardIndex.getBigDecimal(0);
          for (int i = 1; i < numDocs; i++) {
            BigDecimal curr = _forwardIndex.getBigDecimal(i);
            if (curr.compareTo(prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case STRING: {
          String prev = _forwardIndex.getString(0);
          for (int i = 1; i < numDocs; i++) {
            String curr = _forwardIndex.getString(i);
            if (curr.compareTo(prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        case BYTES: {
          byte[] prev = _forwardIndex.getBytes(0);
          for (int i = 1; i < numDocs; i++) {
            byte[] curr = _forwardIndex.getBytes(i);
            if (ByteArray.compare(curr, prev) < 0) {
              return false;
            }
            prev = curr;
          }
          return true;
        }
        default:
          throw new IllegalStateException("Unsupported stored type: " + storedType);
      }
    }
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _dataSourceMetadata.getNumDocs();
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _dataSourceMetadata.getMaxNumValuesPerMVEntry();
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _dataSourceMetadata.getMaxRowLengthInBytes();
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _dataSourceMetadata.getPartitionFunction();
  }

  @Override
  public Set<Integer> getPartitions() {
    return _dataSourceMetadata.getPartitions();
  }

  @Override
  public CLPStats getCLPStats() {
    if (_forwardIndex instanceof CLPMutableForwardIndex) {
      return ((CLPMutableForwardIndex) _forwardIndex).getCLPStats();
    } else if (_forwardIndex instanceof CLPMutableForwardIndexV2) {
      return ((CLPMutableForwardIndexV2) _forwardIndex).getCLPStats();
    }
    throw new IllegalStateException(
        "CLP stats not available for column: " + _dataSourceMetadata.getFieldSpec().getName());
  }

  @Override
  public CLPV2Stats getCLPV2Stats() {
    if (_forwardIndex instanceof CLPMutableForwardIndexV2) {
      return ((CLPMutableForwardIndexV2) _forwardIndex).getCLPV2Stats();
    }
    throw new IllegalStateException(
        "CLPV2 stats not available for column: " + _dataSourceMetadata.getFieldSpec().getName());
  }
}
