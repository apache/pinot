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
package org.apache.pinot.core.data.table;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.util.GroupByUtils;


public class RadixPartitionedIntermediateRecords {
  private final int _numRadixBits;
  private final int _numPartitions;
  private final int _mask;
  private final List<IntermediateRecord> _records;
  private final int _segmentId;
  private final int[] _partitionBoundaries;

  public RadixPartitionedIntermediateRecords(int numRadixBits, int segmentId, List<IntermediateRecord> records) {
    _records = records;
    _numRadixBits = numRadixBits;
    _numPartitions = 1 << numRadixBits;
    _mask = _numPartitions - 1;
    _segmentId = segmentId;
    _partitionBoundaries = new int[_numPartitions + 1];
    partitionInPlace();
  }

  // O(n) in-place partition of _records, updates _partitionBoundaries
  private void partitionInPlace() {
    int[] partitionSizes = new int[_numPartitions];
    for (IntermediateRecord record : _records) {
      int p = partition(record);
      partitionSizes[p]++;
    }
    _partitionBoundaries[0] = 0;
    for (int i = 1; i <= _numPartitions; i++) {
      _partitionBoundaries[i] = _partitionBoundaries[i - 1] + partitionSizes[i - 1];
    }
    assert (_partitionBoundaries[_numPartitions] == _records.size());
    int[] nextIdx = new int[_numPartitions];
    System.arraycopy(_partitionBoundaries, 0, nextIdx, 0, _numPartitions);

    for (int p = 0; p < _numPartitions; p++) {
      while (nextIdx[p] < _partitionBoundaries[p + 1]) {
        int fromIdx = nextIdx[p];
        IntermediateRecord record = _records.get(fromIdx);
        int targetPartition = partition(record);
        int targetIdx = nextIdx[targetPartition];
        Collections.swap(_records, fromIdx, targetIdx);
        nextIdx[targetPartition]++;
      }
    }
  }

  public int partition(IntermediateRecord record) {
    if (record._keyHashCode == -1) {
      record._keyHashCode = hash(record._key);
    }
    return record._keyHashCode & _mask;
  }

  private int hash(Key key) {
    return GroupByUtils.hashForPartition(key);
  }

  public List<IntermediateRecord> getPartition(int partition) {
    return _records.subList(_partitionBoundaries[partition], _partitionBoundaries[partition + 1]);
  }

  public List<IntermediateRecord> getRecordPartition(IntermediateRecord record) {
    int partition = partition(record);
    return getPartition(partition);
  }

  public int getSegmentId() {
    return _segmentId;
  }
}
