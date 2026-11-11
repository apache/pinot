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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/// Column statistics for a column coming from an in-memory realtime segment.
public class MutableColumnStatistics implements ColumnStatistics {
  protected final DataSource _dataSource;
  protected final DataSourceMetadata _dataSourceMetadata;
  protected final FieldSpec _fieldSpec;
  @Nullable
  protected final int[] _sortedDocIds;
  protected final boolean _isSortedColumn;

  // NOTE: For new added columns during the ingestion, this will be constant value dictionary instead of mutable
  //       dictionary.
  protected final Dictionary _dictionary;

  // Lazily computed because it may require a full scan of the forward index, and it is queried multiple times per
  // column during segment creation. Left unsynchronized: an instance describes a single column and is reached only
  // through the per-column stats map, so it is confined to whichever thread creates that column. Even if that ever
  // changes, the segment no longer accepts documents by the time stats are collected, so a race can only recompute
  // the same value.
  private Boolean _sorted;

  public MutableColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIds, boolean isSortedColumn) {
    _dataSource = dataSource;
    _dataSourceMetadata = dataSource.getDataSourceMetadata();
    _fieldSpec = _dataSourceMetadata.getFieldSpec();
    Preconditions.checkState(_dataSourceMetadata.getNumDocs() > 0,
        "Use EmptyColumnStatistics for empty column: %s", _fieldSpec.getName());
    _sortedDocIds = sortedDocIds;
    _isSortedColumn = isSortedColumn;
    _dictionary = dataSource.getDictionary();
    Preconditions.checkState(_dictionary != null, "Failed to find dictionary for column: %s", _fieldSpec.getName());
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
    return (Comparable<?>) _dictionary.getMinVal();
  }

  @Override
  public Comparable<?> getMaxValue() {
    return (Comparable<?>) _dictionary.getMaxVal();
  }

  @Override
  public Object getUniqueValuesSet() {
    return _dictionary.getSortedValues();
  }

  @Override
  public int getCardinality() {
    return _dictionary.length();
  }

  @Override
  public int getLengthOfShortestElement() {
    return _dictionary.getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLongestElement() {
    return _dictionary.getLengthOfLongestElement();
  }

  @Override
  public boolean isAscii() {
    return _dictionary.isAscii();
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

    // A single distinct value is always sorted — no scan needed. Cardinality cannot be 0 here because the segment is
    // non-empty and every document of a dictionary-encoded column has a dict id.
    if (getCardinality() == 1) {
      return true;
    }

    // Iterate over all data to figure out whether or not it's in sorted order
    MutableForwardIndex forwardIndex = (MutableForwardIndex) _dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndex != null, "Failed to find forward index for column: %s", _fieldSpec.getName());
    int numDocs = _dataSourceMetadata.getNumDocs();
    // Iterate with the sorted order if provided
    if (_sortedDocIds != null) {
      int prevDictId = forwardIndex.getDictId(_sortedDocIds[0]);
      for (int i = 1; i < numDocs; i++) {
        int dictId = forwardIndex.getDictId(_sortedDocIds[i]);
        // A repeated dict id cannot break the sort order, so skip the comparison entirely
        if (dictId != prevDictId) {
          if (_dictionary.compare(prevDictId, dictId) > 0) {
            return false;
          }
          prevDictId = dictId;
        }
      }
    } else {
      int prevDictId = forwardIndex.getDictId(0);
      for (int i = 1; i < numDocs; i++) {
        int dictId = forwardIndex.getDictId(i);
        // A repeated dict id cannot break the sort order, so skip the comparison entirely
        if (dictId != prevDictId) {
          if (_dictionary.compare(prevDictId, dictId) > 0) {
            return false;
          }
          prevDictId = dictId;
        }
      }
    }

    return true;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _dataSourceMetadata.getNumValues();
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
}
