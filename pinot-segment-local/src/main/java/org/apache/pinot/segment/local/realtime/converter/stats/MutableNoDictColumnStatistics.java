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


@SuppressWarnings("rawtypes")
public class MutableNoDictColumnStatistics implements ColumnStatistics, CLPStatsProvider {
  protected final DataSourceMetadata _dataSourceMetadata;
  protected final FieldSpec _fieldSpec;
  @Nullable
  protected final int[] _sortedDocIds;
  protected final boolean _isSortedColumn;
  protected final MutableForwardIndex _forwardIndex;

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
  public Comparable getMinValue() {
    return _dataSourceMetadata.getMinValue();
  }

  @Override
  public Comparable getMaxValue() {
    return _dataSourceMetadata.getMaxValue();
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
    // Sorted column is guaranteed to be sorted by construction — no scan needed
    if (_isSortedColumn) {
      return true;
    }

    // Multi-valued column cannot be sorted
    if (!isSingleValue()) {
      return false;
    }

    int numDocs = _dataSourceMetadata.getNumDocs();

    // Verify that values are non-decreasing when iterated in the given order
    DataType valueType = getValueType();
    if (_sortedDocIds != null) {
      switch (valueType) {
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
          throw new IllegalStateException("Unsupported value type: " + valueType);
      }
    } else {
      switch (valueType) {
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
          throw new IllegalStateException("Unsupported value type: " + valueType);
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
