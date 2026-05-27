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
package org.apache.pinot.query.runtime.operator.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorAppender;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.roaringbitmap.RoaringBitmap;


/**
 * A hash table for the <em>build</em> side of an Arrow-native hash join.
 *
 * <p>Usage:
 * <ol>
 *   <li>Call {@link #push(ArrowDataBlock)} for each right-side batch as it arrives.</li>
 *   <li>Call {@link #build()} once all batches have been pushed. This merges all batches into a single
 *       contiguous {@link ArrowDataBlock} and builds the open-addressing linear-probe hash table.</li>
 *   <li>Use {@link #getHashes()}, {@link #getAddresses()}, etc. to hand the table off to {@link ArrowJoinProbe}.</li>
 * </ol>
 *
 * <p>This class is {@link AutoCloseable}; call {@link #close()} to release all off-heap buffers.
 */
public class ArrowLookupTable implements AutoCloseable {
  /** Sentinel value in the hash table marking an empty slot. */
  public static final int SENTINEL = Integer.MIN_VALUE;

  private final KeySelector<?> _keySelector;
  private final float _loadFactor;
  private final BufferAllocator _allocator;
  private final List<ArrowDataBlock> _rightBatches = new ArrayList<>();

  private KeySelector.ArrowKeyHasher _keyHasher;
  private ArrowDataBlock _mergedBlock;
  private int[] _addresses;
  private int[] _hashes;
  private int _size = 0;
  private int _capacity;
  private int _hashMask;
  private boolean _hasCollisions = false;
  private boolean _finalized = false;

  @javax.annotation.Nullable
  private RoaringBitmap _matchedRightRows = null;

  public ArrowLookupTable(KeySelector<?> keySelector, BufferAllocator allocator) {
    this(keySelector, 0.6f, allocator);
  }

  public ArrowLookupTable(KeySelector<?> keySelector, float loadFactor, BufferAllocator allocator) {
    _keySelector = keySelector;
    _loadFactor = loadFactor;
    _allocator = allocator;
  }

  /** Enables tracking of which right-side rows have been matched (needed for RIGHT / FULL OUTER joins). */
  public void enableMatchedRowTracking() {
    if (_matchedRightRows == null) {
      _matchedRightRows = new RoaringBitmap();
    }
  }

  @javax.annotation.Nullable
  public RoaringBitmap getMatchedRightRows() {
    return _matchedRightRows;
  }

  /** Returns a bitmap of unmatched right-side rows (flips the matched bitmap). Invalidates matched tracking. */
  public RoaringBitmap getUnmatchedRightRows() {
    if (_matchedRightRows == null) {
      throw new IllegalStateException("Matched row tracking is not enabled");
    }
    _matchedRightRows.flip(0L, (long) _mergedBlock.getNumberOfRows());
    return _matchedRightRows;
  }

  /**
   * Appends a right-side batch. The hash table is not built until {@link #build()} is called, which avoids
   * any rehashing.
   */
  public void push(ArrowDataBlock dataBlock) {
    if (_finalized) {
      throw new IllegalStateException("Cannot push data blocks after build()");
    }
    _rightBatches.add(dataBlock);
    _size += dataBlock.getNumberOfRows();
  }

  /**
   * Merges all pushed batches into a single block and builds the hash table. Must be called exactly once,
   * after all batches have been pushed.
   */
  public void build() {
    _mergedBlock = mergeBlocks(_rightBatches);

    // Close individual batches that were merged away
    for (ArrowDataBlock batch : _rightBatches) {
      if (batch != _mergedBlock) {
        batch.close();
      }
    }
    _rightBatches.clear();

    if (_mergedBlock == null || _mergedBlock.getNumberOfRows() == 0) {
      _finalized = true;
      return;
    }
    _keyHasher = _keySelector.getArrowHasher(_mergedBlock);

    int initialCapacity = (int) Math.ceil(_size / _loadFactor);
    _capacity = Integer.highestOneBit(Math.max(16, initialCapacity) - 1) << 1;
    _hashMask = _capacity - 1;
    _addresses = new int[_capacity];
    _hashes = new int[_capacity];
    Arrays.fill(_hashes, SENTINEL);

    buildHashTable();
    _finalized = true;
  }

  private void buildHashTable() {
    if (_mergedBlock == null || _mergedBlock.getNumberOfRows() == 0) {
      return;
    }
    int rowCount = _mergedBlock.getNumberOfRows();
    for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      int hash = _keyHasher.computeHash(rowIdx);
      int slot = hash & _hashMask;
      while (_hashes[slot] != SENTINEL) {
        _hasCollisions = true;
        slot = (slot + 1) & _hashMask;
      }
      _addresses[slot] = rowIdx;
      _hashes[slot] = hash;
    }
  }

  /**
   * Merges a list of Arrow batches into one contiguous block, decoding any dictionary-encoded columns so
   * the merged block has a uniform (non-dictionary) schema.
   */
  private ArrowDataBlock mergeBlocks(List<ArrowDataBlock> batches) {
    if (batches.isEmpty()) {
      return null;
    }
    ArrowDataBlock first = batches.get(0);
    if (batches.size() == 1) {
      return first;
    }

    boolean hasDictionary = hasDictionaryColumns(first);
    boolean allShareSameDictionary = hasDictionary && allBatchesShareDictionary(batches);

    Schema mergedSchema;
    if (hasDictionary && !allShareSameDictionary) {
      // Different dictionaries across batches — must decode to plain VarChar
      mergedSchema = buildDecodedSchema(first);
    } else {
      // Either no dictionaries, or all batches share the same one — keep schema as-is
      mergedSchema = first.getSchema();
    }

    VectorSchemaRoot mergedRoot = VectorSchemaRoot.create(mergedSchema, _allocator);
    int numFields = mergedRoot.getFieldVectors().size();
    for (int i = 0; i < numFields; i++) {
      FieldVector target = mergedRoot.getVector(i);
      target.setInitialCapacity(_size);
      target.allocateNew();
      VectorAppender appender = new VectorAppender(target);
      for (ArrowDataBlock batch : batches) {
        FieldVector source = batch.getRoot().getVector(i);
        if (source.getField().getDictionary() != null && batch.getDictionaryProvider() != null) {
          if (allShareSameDictionary) {
            // Same dictionary across all batches — append index vectors directly (no decode)
            source.accept(appender, null);
          } else {
            // Different dictionaries — must decode before appending
            DictionaryProvider provider = batch.getDictionaryProvider();
            Dictionary dictionary = provider.lookup(source.getField().getDictionary().getId());
            try (FieldVector decoded = (FieldVector) DictionaryEncoder.decode(source, dictionary)) {
              decoded.accept(appender, null);
            }
          }
        } else {
          source.accept(appender, null);
        }
      }
    }
    mergedRoot.setRowCount(_size);

    // When all batches share the same dictionary, preserve it on the merged block
    DictionaryProvider mergedDictProvider = allShareSameDictionary
        ? ArrowDataBlock.copyDictionaryProvider(first.getDictionaryProvider(), _allocator)
        : null;
    return new ArrowDataBlock(mergedRoot, mergedDictProvider);
  }

  private static boolean hasDictionaryColumns(ArrowDataBlock block) {
    for (Field field : block.getSchema().getFields()) {
      if (field.getDictionary() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if all batches share the exact same dictionary for every dictionary-encoded column.
   * This is the common case when all batches originate from the same leaf stage.
   */
  private static boolean allBatchesShareDictionary(List<ArrowDataBlock> batches) {
    ArrowDataBlock first = batches.get(0);
    DictionaryProvider firstProvider = first.getDictionaryProvider();
    if (firstProvider == null) {
      return false;
    }
    for (int i = 1; i < batches.size(); i++) {
      DictionaryProvider provider = batches.get(i).getDictionaryProvider();
      if (provider == null) {
        return false;
      }
      // Check that dictionary IDs match and dictionaries have the same size
      // (identity check on the provider object handles the common case of literal sharing)
      if (provider == firstProvider) {
        continue;
      }
      for (long dictId : firstProvider.getDictionaryIds()) {
        Dictionary firstDict = firstProvider.lookup(dictId);
        Dictionary otherDict = provider.lookup(dictId);
        if (otherDict == null
            || firstDict.getVector().getValueCount() != otherDict.getVector().getValueCount()) {
          return false;
        }
      }
    }
    return true;
  }

  private static Schema buildDecodedSchema(ArrowDataBlock block) {
    List<Field> decodedFields = new ArrayList<>();
    for (Field field : block.getSchema().getFields()) {
      if (field.getDictionary() != null) {
        decodedFields.add(Field.nullable(field.getName(), new ArrowType.Utf8()));
      } else {
        decodedFields.add(field);
      }
    }
    return new Schema(decodedFields);
  }

  // ----- accessors for ArrowJoinProbe -----

  public ArrowDataBlock getMergedBlock() {
    return _mergedBlock;
  }

  public int[] getHashes() {
    return _hashes;
  }

  public int[] getAddresses() {
    return _addresses;
  }

  public int getHashMask() {
    return _hashMask;
  }

  public boolean hasCollisions() {
    return _hasCollisions;
  }

  public KeySelector<?> getKeySelector() {
    return _keySelector;
  }

  @Override
  public void close() {
    for (ArrowDataBlock batch : _rightBatches) {
      batch.close();
    }
    if (_mergedBlock != null) {
      _mergedBlock.close();
    }
  }
}
