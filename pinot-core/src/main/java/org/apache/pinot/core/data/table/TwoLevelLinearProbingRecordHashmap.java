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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TwoLevelLinearProbingRecordHashmap {
  private static final int INITIAL_CAP = 128;
  private static final double LOAD_FACTOR = 0.75;
  private static final int BLOCK_SIZE = 256;

  // pointer table: stores indices into payload
  private int[] _pointers;
  private int _ptrsRealSize;
  private int _size;

  // payload storage
  private final List<IntermediateRecord[]> _payloads;

  private int _currBlockId;
  private int _nextBlockOffset;

  public TwoLevelLinearProbingRecordHashmap() {
    _pointers = new int[INITIAL_CAP * 3];
    Arrays.fill(_pointers, -1);
    _ptrsRealSize = INITIAL_CAP;

    _payloads = new ArrayList<>();

    _payloads.add(new IntermediateRecord[BLOCK_SIZE]);

    _currBlockId = 0;
    _nextBlockOffset = 0;
    _size = 0;
  }

  public Record get(Key key) {
    int hash = hash(key);
    int idx = findPointerIndex(key, hash, _pointers, _ptrsRealSize);
    int blockId = _pointers[idx];
    int blockOffset = _pointers[idx + 1];
    if (blockId == -1) {
      return null;
    }
    IntermediateRecord row = _payloads.get(blockId)[blockOffset];
    return row._record;
  }

  public void put(Key key, Record value) {
    if ((double) _size > _ptrsRealSize * LOAD_FACTOR) {
      resize();
    }
    int hash = hash(key);
    int ptrIdx = findPointerIndex(key, hash, _pointers, _ptrsRealSize);
    int blockId = _pointers[ptrIdx];
    int blockOffset = _pointers[ptrIdx + 1];

    if (blockId != -1) {
      // update existing payload
      IntermediateRecord row = _payloads.get(blockId)[blockOffset];
      row._record = value;
    } else {
      // insert new payload entry
      // go to next block if current is filled
      if (_nextBlockOffset == BLOCK_SIZE) {
        growPayloadBlock();
      }
      _payloads.get(_currBlockId)[_nextBlockOffset] = IntermediateRecord.create(key, value, hash);
      _pointers[ptrIdx] = _currBlockId;
      _pointers[ptrIdx + 1] = _nextBlockOffset++;
      _pointers[ptrIdx + 2] = hash;
      _size++;
    }
  }

  // TODO: implement this
  private IntermediateRecord updateRecord(IntermediateRecord existingRecord, IntermediateRecord newRecord) {
    return newRecord;
  }

  public void put(IntermediateRecord record) {
    assert (record._keyHashCode != -1);
    if ((double) _size > _ptrsRealSize * LOAD_FACTOR) {
      resize();
    }
    int ptrIdx = findPointerIndex(record._key, record._keyHashCode, _pointers, _ptrsRealSize);
    int blockId = _pointers[ptrIdx];
    int blockOffset = _pointers[ptrIdx + 1];

    if (blockId != -1) {
      // update existing payload
      _payloads.get(blockId)[blockOffset] = updateRecord(_payloads.get(blockId)[blockOffset], record);
    } else {
      // insert new payload entry
      // go to next block if current is filled
      if (_nextBlockOffset == BLOCK_SIZE) {
        growPayloadBlock();
      }
      _payloads.get(_currBlockId)[_nextBlockOffset] = record;
      _pointers[ptrIdx] = _currBlockId;
      _pointers[ptrIdx + 1] = _nextBlockOffset++;
      _pointers[ptrIdx + 2] = record._keyHashCode;
      _size++;
    }
  }

  private int hash(Key key) {
    return key.hashCode() & 0x7fffffff;
  }

  // linear probing over pointer array
  private int findPointerIndex(Key key, int hash, int[] ptrs, int cap) {
    int idx = hash & (cap - 1);
    int realIdx;
    while (true) {
      realIdx = idx * 3;
      int blockId = ptrs[realIdx];
      int blockOffset = ptrs[realIdx + 1];
      int salt = ptrs[realIdx + 2];
      if (blockId == -1) {
        return realIdx;
      }
      if (hash != salt) {
        idx = (idx + 1) & (cap - 1);
        continue;
      }
      Key existing = _payloads.get(blockId)[blockOffset]._key;
      if (existing.equals(key)) {
        return realIdx;
      }
      idx = (idx + 1) & (cap - 1);
    }
  }

  private void growPayloadBlock() {
    // init a new block to double the size
    _payloads.add(new IntermediateRecord[BLOCK_SIZE]);
    _currBlockId++;
    _nextBlockOffset = 0;
  }

  private void resize() {
    int newCap = _ptrsRealSize * 2;
    int[] newPtrs = new int[newCap * 3];
    Arrays.fill(newPtrs, -1);

    // reinsert pointers using stored hash->payload
    for (int blockId = 0; blockId <= _currBlockId; blockId++) {
      int offsetLimit = blockId == _currBlockId ? _nextBlockOffset : BLOCK_SIZE;
      IntermediateRecord[] block = _payloads.get(blockId);
      for (int blockOffset = 0; blockOffset < offsetLimit; blockOffset++) {
        int h = block[blockOffset]._keyHashCode;
        int idx = h & (newCap - 1);
        int realIdx = idx * 3;
        while (newPtrs[realIdx] != -1) {
          idx = (idx + 1) & (newCap - 1);
          realIdx = idx * 3;
        }
        newPtrs[realIdx] = blockId;
        newPtrs[realIdx + 1] = blockOffset;
        newPtrs[realIdx + 2] = h;
      }
    }

    _pointers = newPtrs;
    _ptrsRealSize = newCap;
  }

  public int size() {
    return _size;
  }
}
