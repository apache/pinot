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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.ws.rs.NotSupportedException;
import org.apache.arrow.util.Preconditions;
import org.apache.pinot.core.util.GroupByUtils;


/**
 * Two-level linear probing hashmap for group by execution
 * See docs below for details
 */
public class TwoLevelLinearProbingRecordHashMap {
  ///  Because fast modulo is used, {@code INITIAL_CAP} must be power of 2
  private static final int INITIAL_CAP = 128;
  private static final double LOAD_FACTOR = 0.75;
  ///  Block size is separate as pointer array size, and they grow separately.
  private static final int BLOCK_SIZE = 256;

  /**
   * <p>Pointer table: stores indices into payloads</p>
   * <p>{@code _pointers} is <b><i>logically</i></b> an {@code int[_ptrsRealSize][3]} array,
   * where each row represents a pointer to the actual payload, and the column layout: </p>
   * <p> 0: blockId, 1: blockOffset, 2: salt (payload key's hash value).</p>
   * <p>Salt is kept in the pointer array for less jumping to payload block
   * when comparing during linear probing</p>
   * <p>{@code _ptrsRealSize} is the logical size of the pointer array that should be used
   * when probing. For performance, the pointer array is <b><i>flattened</i></b> and hence
   * the actual length of {@code _pointers} is {@code 3 * _ptrsRealSize}</p>
   */
  private int[] _pointers;
  private int _ptrsRealSize;
  private int _size;
  private final int _initialCap;

  /// Payload block storage
  private final List<IntermediateRecord[]> _payloads;

  private int _currBlockId;
  private int _nextBlockOffset;

  public TwoLevelLinearProbingRecordHashMap() {
    _initialCap = INITIAL_CAP;
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

  public Record put(Key key, Record value) {
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
      Record prev = row._record;
      row._record = value;
      return prev;
    }
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
    return null;
  }

  public void compute(IntermediateRecord record, Function<IntermediateRecord, IntermediateRecord> remappingFunction) {
    if ((double) _size > _ptrsRealSize * LOAD_FACTOR) {
      resize();
    }
    int ptrIdx = findPointerIndex(record._key, record._keyHashCode, _pointers, _ptrsRealSize);
    int blockId = _pointers[ptrIdx];
    if (blockId == -1) {
      // put new value
      IntermediateRecord newRecord = remappingFunction.apply(null);
      putNew(newRecord, ptrIdx);
      return;
    }
    int blockOffset = _pointers[ptrIdx + 1];
    IntermediateRecord oldValue = _payloads.get(blockId)[blockOffset];
    // This should update old record in-place
    remappingFunction.apply(oldValue);
  }

  public void computeIfPresent(IntermediateRecord record,
      Function<IntermediateRecord, IntermediateRecord> remappingFunction) {
    int ptrIdx = findPointerIndex(record._key, record._keyHashCode, _pointers, _ptrsRealSize);
    int blockId = _pointers[ptrIdx];
    // if no existing found, return null
    if (blockId == -1) {
      return;
    }
    int blockOffset = _pointers[ptrIdx + 1];
    IntermediateRecord oldValue = _payloads.get(blockId)[blockOffset];
    // This should update old record in-place
    remappingFunction.apply(oldValue);
  }

  public boolean containsKey(Key key) {
    int ptrIdx = findPointerIndex(key, hash(key), _pointers, _ptrsRealSize);
    return _pointers[ptrIdx] != -1;
  }

  public void put(IntermediateRecord record) {
    Preconditions.checkState(record._keyHashCode == hash(record._key));
    if ((double) _size > _ptrsRealSize * LOAD_FACTOR) {
      resize();
    }
    int ptrIdx = findPointerIndex(record._key, record._keyHashCode, _pointers, _ptrsRealSize);
    int blockId = _pointers[ptrIdx];
    int blockOffset = _pointers[ptrIdx + 1];

    if (blockId != -1) {
      // update existing payload
      _payloads.get(blockId)[blockOffset] = record;
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

  /// Put non-existing record
  private void putNew(IntermediateRecord record, int ptrIdx) {
    Preconditions.checkState(record._keyHashCode == hash(record._key));
    assert (record._keyHashCode != -1);
    Preconditions.checkState(_pointers[ptrIdx] == -1);
    Preconditions.checkState(_pointers[ptrIdx + 1] == -1);
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

  private int hash(Key key) {
    return GroupByUtils.hashForPartition(key);
  }

  /// Linear probing over pointer array
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

  public void clear() {
    _pointers = new int[_initialCap * 3];
    Arrays.fill(_pointers, -1);
    _ptrsRealSize = _initialCap;

    _payloads.clear();
    _payloads.add(new IntermediateRecord[BLOCK_SIZE]);

    _currBlockId = 0;
    _nextBlockOffset = 0;
    _size = 0;
  }

  public Iterator<IntermediateRecord> iterator() {
    return new RecordIterator();
  }

  public List<Record> values() {
    Record[] values = new Record[_size];
    Iterator<IntermediateRecord> it = iterator();
    int idx = 0;
    while (it.hasNext()) {
      values[idx++] = it.next()._record;
    }
    return Arrays.asList(values);
  }

  public List<Key> keys() {
    List<Key> keys = new ArrayList<>(_size);
    Iterator<IntermediateRecord> it = iterator();
    while (it.hasNext()) {
      keys.add(it.next()._key);
    }
    return keys;
  }

  public List<Record> getPayloads() {
    return new PayLoadWrapperList();
  }

  /// Wrapper for list view over the block payloads
  public class PayLoadWrapperList extends AbstractList<Record> {
    @Override
    public Record get(int i) {
      int blockId = i / BLOCK_SIZE;
      int blockOffset = i & (BLOCK_SIZE - 1);
      return _payloads.get(blockId)[blockOffset]._record;
    }

    @Override
    public int size() {
      return _size;
    }
  }

  public class RecordIterator implements Iterator<IntermediateRecord> {
    private int _blockId;
    private int _blockOffset;

    RecordIterator() {
      _blockId = 0;
      _blockOffset = 0;
    }

    @Override
    public boolean hasNext() {
      return _blockId < _currBlockId || (_blockId == _currBlockId && _blockOffset < _nextBlockOffset);
    }

    @Override
    public IntermediateRecord next() {
      IntermediateRecord rec = _payloads.get(_blockId)[_blockOffset++];
      if (_blockOffset == BLOCK_SIZE) {
        _blockOffset = 0;
        _blockId++;
      }
      if (_blockId > _currBlockId) {
        Preconditions.checkState(!hasNext());
      }
      return rec;
    }

    @Override
    public void remove() {
      throw new NotSupportedException();
    }

    @Override
    public void forEachRemaining(Consumer<? super IntermediateRecord> action) {
      throw new NotSupportedException();
    }
  }
}
