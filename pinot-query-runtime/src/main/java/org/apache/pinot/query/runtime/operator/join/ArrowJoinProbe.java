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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Executes the <em>probe</em> phase of an Arrow-native hash join against an {@link ArrowLookupTable}.
 *
 * <p>The probe phase iterates over left-side rows, computes their hash, locates matching right-side rows
 * in the linear-probe table, and materialises the joined output as a new {@link ArrowDataBlock}.
 *
 * <p>All data copies use Arrow's {@link FieldVector#copyFromSafe} which operates directly on off-heap buffers.
 */
public class ArrowJoinProbe {
  private final ArrowDataBlock _leftBlock;
  private final ArrowDataBlock _rightBlock;
  private final int[] _hashes;
  private final int[] _addresses;
  private final int _hashMask;
  private final boolean _hasCollisions;
  private final KeySelector.ArrowKeyHasher _leftKeyHasher;
  private final KeySelector.ArrowKeyComparator _keyComparator;
  @Nullable
  private final TransformOperand.ArrowEvaluator[] _nonEquiEvaluators;
  @Nullable
  private final RoaringBitmap _matchedRightRows;
  private final int _leftColumnCount;
  private final int _resultColumnCount;
  private final BufferAllocator _allocator;
  // Shared across all result blocks to avoid deep-copying dictionaries per result.
  // Null when the left block has no dictionaries (common path via ArrowBlockConverter).
  @Nullable
  private final ArrowDataBlock.SharedDictionaryProvider _sharedDictProvider;

  public ArrowJoinProbe(ArrowDataBlock leftBlock, KeySelector<?> leftSelector, ArrowLookupTable rightTable,
      @Nullable List<TransformOperand> nonEquiEvaluators, int leftColumnCount, int resultColumnCount,
      BufferAllocator allocator) {
    _leftBlock = leftBlock;
    _rightBlock = rightTable.getMergedBlock();
    _hashes = rightTable.getHashes();
    _addresses = rightTable.getAddresses();
    _hashMask = rightTable.getHashMask();
    _hasCollisions = rightTable.hasCollisions();
    _matchedRightRows = rightTable.getMatchedRightRows();
    _leftKeyHasher = leftSelector.getArrowHasher(leftBlock);
    _keyComparator = leftSelector.getArrowKeyComparator(leftBlock, _rightBlock, rightTable.getKeySelector());
    _leftColumnCount = leftColumnCount;
    _allocator = allocator;
    _resultColumnCount = resultColumnCount;
    _sharedDictProvider = ArrowDataBlock.shareDictionaryProvider(leftBlock.getDictionaryProvider());

    if (nonEquiEvaluators != null && !nonEquiEvaluators.isEmpty()) {
      _nonEquiEvaluators = new TransformOperand.ArrowEvaluator[nonEquiEvaluators.size()];
      for (int i = 0; i < nonEquiEvaluators.size(); i++) {
        _nonEquiEvaluators[i] = nonEquiEvaluators.get(i).createArrowEvaluator(leftBlock, _rightBlock);
      }
    } else {
      _nonEquiEvaluators = null;
    }
  }

  // ----- public API -----

  /**
   * Builds the result of an INNER join: only rows with at least one match on both sides are emitted.
   */
  public ArrowDataBlock buildOnlyMatches() {
    IntArrayList leftIndexes = new IntArrayList();
    IntArrayList rightIndexes = new IntArrayList();
    collectAllMatches(leftIndexes, rightIndexes);

    VectorSchemaRoot result = VectorSchemaRoot.create(resultSchema(), _allocator);
    if (leftIndexes.isEmpty()) {
      return new ArrowDataBlock(result);
    }
    int rowCount = leftIndexes.size();
    copyColumns(_leftBlock, leftIndexes, result, 0, _leftColumnCount, rowCount);
    copyColumns(_rightBlock, rightIndexes, result, _leftColumnCount, _resultColumnCount - _leftColumnCount, rowCount);
    result.setRowCount(rowCount);
    return new ArrowDataBlock(result, retainSharedDict());
  }

  /**
   * Builds the result of a LEFT OUTER join: all left rows are emitted, with null right columns for non-matches.
   */
  public ArrowDataBlock buildWithUnmatchedLeft() {
    IntArrayList leftIndexes = new IntArrayList();
    IntArrayList rightIndexes = new IntArrayList();
    collectAllMatches(leftIndexes, rightIndexes);

    int leftRowCount = _leftBlock.getNumberOfRows();
    VectorSchemaRoot result = VectorSchemaRoot.create(resultSchema(), _allocator);

    // Build a lookup: leftRow → first match index (for interleaving)
    int totalRows = computeTotalRowsWithUnmatchedLeft(leftIndexes, leftRowCount);
    List<FieldVector> resultVectors = result.getFieldVectors();
    allocateVectors(resultVectors, totalRows);

    int currentRow = 0;
    int matchIdx = 0;
    for (int leftRow = 0; leftRow < leftRowCount; leftRow++) {
      if (matchIdx >= leftIndexes.size() || leftIndexes.getInt(matchIdx) != leftRow) {
        // No match — emit left columns, leave right columns null
        for (int col = 0; col < _leftColumnCount; col++) {
          resultVectors.get(col).copyFromSafe(leftRow, currentRow, _leftBlock.getRoot().getVector(col));
        }
        currentRow++;
      } else {
        while (matchIdx < leftIndexes.size() && leftIndexes.getInt(matchIdx) == leftRow) {
          for (int col = 0; col < _leftColumnCount; col++) {
            resultVectors.get(col).copyFromSafe(leftRow, currentRow, _leftBlock.getRoot().getVector(col));
          }
          int rightRow = rightIndexes.getInt(matchIdx);
          for (int col = 0; col < _resultColumnCount - _leftColumnCount; col++) {
            resultVectors.get(_leftColumnCount + col).copyFromSafe(rightRow, currentRow,
                _rightBlock.getRoot().getVector(col));
          }
          currentRow++;
          matchIdx++;
        }
      }
    }
    result.setRowCount(currentRow);
    return new ArrowDataBlock(result, retainSharedDict());
  }

  /**
   * Builds the result of a SEMI join: left rows that have at least one match on the right side.
   */
  public ArrowDataBlock buildSemiJoin() {
    RoaringBitmap matched = matchedLeftRows();
    return buildLeftSubset(matched);
  }

  /**
   * Builds the result of an ANTI join: left rows that have NO match on the right side.
   */
  public ArrowDataBlock buildAntiJoin() {
    RoaringBitmap matched = matchedLeftRows();
    matched.flip(0L, (long) _leftBlock.getNumberOfRows());
    return buildLeftSubset(matched);
  }

  /** Collects left/right row-index pairs for all equi-join (and non-equi) matches. */
  public void collectAllMatches(IntArrayList leftIndexes, IntArrayList rightIndexes) {
    if (_hasCollisions) {
      collectMatchesWithCollisions(leftIndexes, rightIndexes);
    } else {
      collectMatchesNoCollisions(leftIndexes, rightIndexes);
    }
    if (_matchedRightRows != null) {
      for (int i = 0; i < rightIndexes.size(); i++) {
        _matchedRightRows.add(rightIndexes.getInt(i));
      }
    }
  }

  /** Returns a bitmap of left-side row indices that have at least one match. */
  public RoaringBitmap matchedLeftRows() {
    return _hasCollisions ? matchedLeftRowsWithCollisions() : matchedLeftRowsNoCollisions();
  }

  // ----- private helpers -----

  /** Full result schema: left columns + right columns. */
  private Schema resultSchema() {
    List<Field> fields = new ArrayList<>(_resultColumnCount);
    List<Field> leftFields = _leftBlock.getSchema().getFields();
    List<Field> rightFields = _rightBlock != null ? _rightBlock.getSchema().getFields() : List.of();
    for (int i = 0; i < _leftColumnCount; i++) {
      fields.add(leftFields.get(i));
    }
    for (int i = 0; i < _resultColumnCount - _leftColumnCount; i++) {
      fields.add(rightFields.get(i));
    }
    return new Schema(fields);
  }

  /** Left-only result schema: used by SEMI and ANTI joins which only return left-side columns. */
  private Schema leftOnlySchema() {
    List<Field> leftFields = _leftBlock.getSchema().getFields();
    return new Schema(leftFields.subList(0, _leftColumnCount));
  }

  private ArrowDataBlock buildLeftSubset(RoaringBitmap rowIds) {
    VectorSchemaRoot root = VectorSchemaRoot.create(leftOnlySchema(), _allocator);
    int cardinality = rowIds.getCardinality();
    int[] buffer = new int[256];
    for (int col = 0; col < _leftColumnCount; col++) {
      FieldVector source = _leftBlock.getRoot().getVector(col);
      FieldVector target = root.getVector(col);
      target.setInitialCapacity(cardinality);
      target.allocateNew();
      RoaringBatchIterator it = rowIds.getBatchIterator();
      int targetRow = 0;
      while (it.hasNext()) {
        int batch = it.nextBatch(buffer);
        for (int i = 0; i < batch; i++) {
          target.copyFromSafe(buffer[i], targetRow++, source);
        }
      }
      target.setValueCount(cardinality);
    }
    root.setRowCount(cardinality);
    return new ArrowDataBlock(root, retainSharedDict());
  }

  private void copyColumns(ArrowDataBlock src, IntArrayList rowIndexes, VectorSchemaRoot dest,
      int destColOffset, int numCols, int rowCount) {
    for (int col = 0; col < numCols; col++) {
      FieldVector source = src.getRoot().getVector(col);
      FieldVector target = dest.getVector(destColOffset + col);
      target.setInitialCapacity(rowCount);
      target.allocateNew();
      gatherRows(source, target, rowIndexes, rowCount);
    }
  }

  /**
   * Copies selected rows from {@code source} to {@code target} using the fastest available path.
   *
   * <p>For fixed-width vectors (INT, LONG, FLOAT, DOUBLE), this operates directly on the underlying
   * {@link ArrowBuf} data buffers, bypassing per-row bounds checking and validity bit logic. This is
   * significantly faster than {@link FieldVector#copyFromSafe} for large row counts.
   *
   * <p>Variable-width vectors (VarChar, VarBinary, List) fall back to {@code copyFromSafe}.
   */
  private static void gatherRows(FieldVector source, FieldVector target, IntArrayList indices,
      int rowCount) {
    if (source instanceof BaseFixedWidthVector && target instanceof BaseFixedWidthVector
        && source.getNullCount() == 0) {
      // Fast path: bulk copy data bytes + set all validity bits (no nulls in source)
      int typeWidth = ((BaseFixedWidthVector) source).getTypeWidth();
      ArrowBuf srcBuf = source.getDataBuffer();
      ArrowBuf dstBuf = target.getDataBuffer();
      for (int i = 0; i < indices.size(); i++) {
        dstBuf.setBytes((long) i * typeWidth, srcBuf,
            (long) indices.getInt(i) * typeWidth, typeWidth);
      }
      ArrowBuf validityBuf = target.getValidityBuffer();
      int fullBytes = rowCount / 8;
      for (int i = 0; i < fullBytes; i++) {
        validityBuf.setByte(i, 0xFF);
      }
      int remaining = rowCount % 8;
      if (remaining > 0) {
        validityBuf.setByte(fullBytes, (1 << remaining) - 1);
      }
      target.setValueCount(rowCount);
    } else {
      // Fallback: handles variable-width vectors, nullable columns, and type mismatches
      for (int i = 0; i < indices.size(); i++) {
        target.copyFromSafe(indices.getInt(i), i, source);
      }
      target.setValueCount(rowCount);
    }
  }

  private void allocateVectors(List<FieldVector> vectors, int capacity) {
    for (FieldVector vector : vectors) {
      vector.setInitialCapacity(capacity);
      vector.allocateNew();
    }
  }

  private int computeTotalRowsWithUnmatchedLeft(IntArrayList leftIndexes, int leftRowCount) {
    // Rows = all left rows + additional rows for multi-matches
    int total = leftRowCount;
    int prev = -1;
    for (int i = 0; i < leftIndexes.size(); i++) {
      int leftRow = leftIndexes.getInt(i);
      if (leftRow == prev) {
        total++; // extra row for duplicate match
      }
      prev = leftRow;
    }
    return total;
  }

  private boolean matchNonEqui(int leftIdx, int rightIdx) {
    if (_nonEquiEvaluators == null) {
      return true;
    }
    for (TransformOperand.ArrowEvaluator evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(leftIdx, rightIdx))) {
        return false;
      }
    }
    return true;
  }

  private void collectMatchesNoCollisions(IntArrayList leftIndexes, IntArrayList rightIndexes) {
    int leftRowCount = _leftBlock.getNumberOfRows();
    for (int leftRow = 0; leftRow < leftRowCount; leftRow++) {
      int leftHash = _leftKeyHasher.computeHash(leftRow);
      int slot = leftHash & _hashMask;
      if (_hashes[slot] == leftHash) {
        int rightRow = _addresses[slot];
        if (_keyComparator.equals(leftRow, rightRow) && matchNonEqui(leftRow, rightRow)) {
          leftIndexes.add(leftRow);
          rightIndexes.add(rightRow);
        }
      }
    }
  }

  private void collectMatchesWithCollisions(IntArrayList leftIndexes, IntArrayList rightIndexes) {
    int leftRowCount = _leftBlock.getNumberOfRows();
    for (int leftRow = 0; leftRow < leftRowCount; leftRow++) {
      int leftHash = _leftKeyHasher.computeHash(leftRow);
      int slot = leftHash & _hashMask;
      while (true) {
        int rightHash = _hashes[slot];
        if (rightHash == ArrowLookupTable.SENTINEL) {
          break;
        }
        if (rightHash == leftHash) {
          int rightRow = _addresses[slot];
          if (_keyComparator.equals(leftRow, rightRow) && matchNonEqui(leftRow, rightRow)) {
            leftIndexes.add(leftRow);
            rightIndexes.add(rightRow);
          }
        }
        slot = (slot + 1) & _hashMask;
      }
    }
  }

  private RoaringBitmap matchedLeftRowsNoCollisions() {
    RoaringBitmap result = new RoaringBitmap();
    int leftRowCount = _leftBlock.getNumberOfRows();
    for (int leftRow = 0; leftRow < leftRowCount; leftRow++) {
      int leftHash = _leftKeyHasher.computeHash(leftRow);
      int slot = leftHash & _hashMask;
      if (_hashes[slot] == leftHash) {
        int rightRow = _addresses[slot];
        if (_keyComparator.equals(leftRow, rightRow) && matchNonEqui(leftRow, rightRow)) {
          result.add(leftRow);
        }
      }
    }
    return result;
  }

  private RoaringBitmap matchedLeftRowsWithCollisions() {
    RoaringBitmap result = new RoaringBitmap();
    int leftRowCount = _leftBlock.getNumberOfRows();
    for (int leftRow = 0; leftRow < leftRowCount; leftRow++) {
      int leftHash = _leftKeyHasher.computeHash(leftRow);
      int slot = leftHash & _hashMask;
      while (true) {
        int rightHash = _hashes[slot];
        if (rightHash == ArrowLookupTable.SENTINEL) {
          break;
        }
        if (rightHash == leftHash) {
          int rightRow = _addresses[slot];
          if (_keyComparator.equals(leftRow, rightRow) && matchNonEqui(leftRow, rightRow)) {
            result.add(leftRow);
            break;
          }
        }
        slot = (slot + 1) & _hashMask;
      }
    }
    return result;
  }

  /**
   * Returns the shared dictionary provider with an incremented ref count (or null if none).
   * The returned provider can be passed to {@link ArrowDataBlock} without deep-copying.
   */
  @Nullable
  private DictionaryProvider retainSharedDict() {
    if (_sharedDictProvider != null) {
      _sharedDictProvider.retain();
      return _sharedDictProvider;
    }
    return null;
  }
}
