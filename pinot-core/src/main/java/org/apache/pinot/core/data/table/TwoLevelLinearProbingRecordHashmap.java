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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.ws.rs.NotSupportedException;
import org.apache.arrow.util.Preconditions;


public class TwoLevelLinearProbingRecordHashmap {
  private static final int INITIAL_CAP = 128;
  private static final double LOAD_FACTOR = 0.75;
  private static final int BLOCK_SIZE = 256;

  // pointer table: stores indices into payload
  private int[] _pointers;
  private int _ptrsRealSize;
  private int _size;
  private final int _initialCap;

  // payload storage
  private final List<IntermediateRecord[]> _payloads;

  private int _currBlockId;
  private int _nextBlockOffset;

  public TwoLevelLinearProbingRecordHashmap() {
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

//  public TwoLevelLinearProbingRecordHashmap(int initialCapacity) {
//    // initialCapacity need to be power of 2
//    Preconditions.checkState((initialCapacity > 0) && ((initialCapacity & (initialCapacity - 1)) == 0));
//    _initialCap = initialCapacity;
//    _pointers = new int[initialCapacity * 3];
//    Arrays.fill(_pointers, -1);
//    _ptrsRealSize = initialCapacity;
//
//    _payloads = new ArrayList<>();
//
//    _payloads.add(new IntermediateRecord[BLOCK_SIZE]);
//
//    _currBlockId = 0;
//    _nextBlockOffset = 0;
//    _size = 0;
//  }

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

  public Set<Map.Entry<Key, Record>> entrySet() {
    throw new NotSupportedException();
//    Set<Map.Entry<Key, Record>> entrySet = new HashSet<>();
//    for (int blockId = 0; blockId <= _currBlockId; blockId++) {
//      int offsetLimit = blockId == _currBlockId ? _nextBlockOffset : BLOCK_SIZE;
//      IntermediateRecord[] block = _payloads.get(blockId);
//      for (int blockOffset = 0; blockOffset < offsetLimit; blockOffset++) {
//        entrySet.add(new Entry(block[blockOffset]._key, block[blockOffset]._record));
//      }
//    }
//    return entrySet;
  }

  Record compute(Key key, BiFunction<Key, Record, Record> remappingFunction) {
    Record oldValue = this.get(key);
    Record newValue = remappingFunction.apply(key, oldValue);
    assert (newValue != null);
    this.put(key, newValue);
    return newValue;
  }

  Record computeIfPresent(Key key, BiFunction<Key, Record, Record> remappingFunction) {
    Record oldValue = this.get(key);
    if (oldValue == null) {
      return null;
    }
    Record newValue = remappingFunction.apply(key, oldValue);
    assert (newValue != null);
    this.put(key, newValue);
    return newValue;
  }

  boolean containsKey(Key key) {
    return get(key) != null;
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
    List<Record> values = new ArrayList<>();
    Iterator<IntermediateRecord> it = iterator();
    while (it.hasNext()) {
      values.add(it.next()._record);
    }
    return values;
  }

  public List<Key> keys() {
    List<Key> keys = new ArrayList<>();
    Iterator<IntermediateRecord> it = iterator();
    while (it.hasNext()) {
      keys.add(it.next()._key);
    }
    return keys;
  }

  public List<Record> getPayloads() {
    return new PayLoadWrapperList();
  }

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

  public static class Entry implements Map.Entry<Key, Record> {
    private final Key _key;
    private Record _value;

    public Entry(Key key, Record value) {
      _key = key;
      _value = value;
    }

    @Override
    public Key getKey() {
      return _key;
    }

    @Override
    public Record getValue() {
      return _value;
    }

    @Override
    public Record setValue(Record value) {
      Record old = _value;
      _value = value;
      return old;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> other = (Map.Entry<?, ?>) o;
      return Objects.equals(_key, other.getKey())
          && Objects.equals(_value, other.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(_key) ^ Objects.hashCode(_value);
    }

    @Override
    public String toString() {
      return _key + "=" + _value;
    }
  }
}
