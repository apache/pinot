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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.Utf8Utils;


/**
 * Column statistics for a column coming from an in-memory realtime segment.
 *
 * TODO: Gather more info on the fly to avoid scanning the segment
 */
@SuppressWarnings("rawtypes")
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

  private int _minElementLength = -1;
  private int _maxElementLength = -1;
  private boolean _isAscii;

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
  public Comparable getMinValue() {
    return _dictionary.getMinVal();
  }

  @Override
  public Comparable getMaxValue() {
    return _dictionary.getMaxVal();
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
    collectElementLengthIfNeeded();
    return _minElementLength;
  }

  @Override
  public int getLengthOfLongestElement() {
    collectElementLengthIfNeeded();
    return _maxElementLength;
  }

  @Override
  public boolean isAscii() {
    collectElementLengthIfNeeded();
    return _isAscii;
  }

  private void collectElementLengthIfNeeded() {
    if (_minElementLength >= 0) {
      return;
    }

    DataType valueType = _dictionary.getValueType();
    if (valueType.isFixedWidth()) {
      int length = valueType.size();
      _minElementLength = length;
      _maxElementLength = length;
    } else {
      // If the stored type is not fixed width, iterate over the dictionary to find the min/max element length
      // TODO: Collect these stats within Dictionary to avoid an extra scan
      _minElementLength = Integer.MAX_VALUE;
      _maxElementLength = 0;
      boolean isAscii = valueType == DataType.STRING;
      int length = _dictionary.length();
      for (int i = 0; i < length; i++) {
        if (isAscii) {
          byte[] bytes = _dictionary.getBytesValue(i);
          _minElementLength = Math.min(_minElementLength, bytes.length);
          _maxElementLength = Math.max(_maxElementLength, bytes.length);
          isAscii = Utf8Utils.isAscii(bytes);
        } else {
          int elementLength = _dictionary.getValueSize(i);
          _minElementLength = Math.min(_minElementLength, elementLength);
          _maxElementLength = Math.max(_maxElementLength, elementLength);
        }
      }
      _isAscii = isAscii;
    }
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

    // Iterate over all data to figure out whether or not it's in sorted order
    MutableForwardIndex forwardIndex = (MutableForwardIndex) _dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndex != null, "Failed to find forward index for column: %s", _fieldSpec.getName());
    int numDocs = _dataSourceMetadata.getNumDocs();
    // Iterate with the sorted order if provided
    if (_sortedDocIds != null) {
      int previousDictId = forwardIndex.getDictId(_sortedDocIds[0]);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = forwardIndex.getDictId(_sortedDocIds[i]);
        if (_dictionary.compare(previousDictId, currentDictId) > 0) {
          return false;
        }
        previousDictId = currentDictId;
      }
    } else {
      int previousDictId = forwardIndex.getDictId(0);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = forwardIndex.getDictId(i);
        if (_dictionary.compare(previousDictId, currentDictId) > 0) {
          return false;
        }
        previousDictId = currentDictId;
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
