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
package org.apache.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// [GroupKeyGenerator] for GROUP BY GROUPING SETS / ROLLUP / CUBE queries in the single-stage engine.
///
/// Given the union of all grouping columns `[c_0, ..., c_{k-1}]` and a list of grouping sets (each a subset
/// of the union), this generator expands every input row into one group per grouping set in a single scan.
/// For a grouping set `S`, the composite group key is
///
/// ```
/// [ id(c_0) or ID_FOR_NULL, ..., id(c_{k-1}) or ID_FOR_NULL, groupingId ]
/// ```
///
/// where a column `c_i` that does NOT participate in `S` (rolled up) is pinned to the NULL sentinel, and the
/// trailing `groupingId` is the ordinal of `S` (its index in the query's grouping-set list). The ordinal is
/// appended as a synthetic key column so that rows from different grouping sets never collide -- e.g. set
/// `{a}` with `b` rolled up to NULL stays distinct from set `{a, b}` where `b` is genuinely NULL, even though
/// both render `(a, NULL)` -- and it identifies the producing set for GROUPING() / GROUPING_ID(). Because the
/// ordinal is not a per-column bitmask, the number of grouping columns is unlimited.
///
/// This generator always emits multiple keys per row, so it is only used via the multi-value
/// (`int[][]`) executor path. Each union column's value is resolved to an id: dictionary-encoded columns use
/// the segment's native dict-ids directly (fast path), while raw columns (or any column when null handling is
/// enabled) fall back to per-column on-the-fly dictionaries (like [NoDictionaryMultiColumnGroupKeyGenerator])
/// so that NULL keys -- which grouping sets produce regardless of the query's null-handling option -- are
/// representable and reconstructable.
///
/// When every union column is dictionary-encoded and the whole composite key (all column ids plus the set
/// ordinal) fits in 64 bits, keys are packed into a primitive `long` and stored in a [Long2IntOpenHashMap],
/// avoiding the per-group `FixedIntArray` allocation and array hashing of the general
/// [Object2IntOpenHashMap] path (mirrors [DictionaryBasedGroupKeyGenerator]'s LongMapBasedHolder). Otherwise
/// the general path is used.
///
/// A multi-value union column participating in a grouping set expands the row over its values (Cartesian
/// product across participating MV columns), composed with the per-set expansion. Star-tree is not used for
/// grouping-set queries (the planner falls back to the regular path).
public class GroupingSetsGroupKeyGenerator implements GroupKeyGenerator {
  /// Sentinel id for a NULL value or a rolled-up (non-participating) column. Distinct from real dictionary ids
  /// (>= 0) and from GroupKeyGenerator.INVALID_ID (-1), so the three id spaces never collide in a composite key.
  private static final int ID_FOR_NULL = INVALID_ID - 1;

  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final DataType[] _storedTypes;
  /// Segment-native dictionary per union column when the column is dictionary-encoded and can be resolved
  /// through native dict-ids (null-handling disabled); `null` for columns that fall back to an on-the-fly
  /// dictionary. When set, [#_onTheFlyDictionaries] at the same index is `null` and vice versa.
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final boolean[] _isSingleValue;
  private final boolean _hasMvColumn;
  private final boolean _nullHandlingEnabled;
  private final int _numGroupsLimit;

  /// Per grouping set: membership over the union columns. The set's ordinal (its index in the query's
  /// grouping-set list) is the value stored in the synthetic $groupingId key column, keeping rows from
  /// different sets distinct and identifying the set for GROUPING() / GROUPING_ID().
  private final boolean[][] _setContains;
  private final int _numSets;

  /// Group-key store when the composite key does NOT fit in a `long` (see [#_longPacking]). Keys are
  /// [FixedIntArray] wrappers over the per-column ids plus the trailing set ordinal.
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;

  /// When `true`, every union column is dictionary-encoded and the whole composite key -- all column ids plus
  /// the set ordinal -- fits in 64 bits, so group keys are packed into a primitive `long` and stored in
  /// [#_longGroupKeyMap]. This avoids the per-group `FixedIntArray` + backing `int[]` allocation and array
  /// hashing that dominate the [Object2IntOpenHashMap] path (mirrors [DictionaryBasedGroupKeyGenerator]'s
  /// LongMapBasedHolder). When `false`, [#_groupKeyMap] is used instead.
  private final boolean _longPacking;
  private final Long2IntOpenHashMap _longGroupKeyMap;
  /// Bit shift applied to each union column's id when packing, and to the set ordinal in the trailing slot
  /// ([#_bitShifts] has length `_numGroupByExpressions + 1`). Only populated when [#_longPacking] is `true`.
  private final int[] _bitShifts;
  /// Per-column reserved id used for a NULL value or a rolled-up (non-participating) column when long-packing.
  /// Equal to the column's dictionary cardinality, so it never collides with a real dict-id (`0..card-1`).
  private final int[] _nullPackedIds;

  /// Reusable single-element id list for a rolled-up (non-participating) column on the MV path. Per-instance
  /// (segment-confined, never shared across query threads) and treated as read-only: expandGroupIds copies its
  /// element into the composite key and never mutates it.
  /// Stands for a NULL key component, whether because the grouping set excludes the column or because the row's
  /// value is NULL. Shared by every row and set: the key expansion reads it and never writes to it.
  private final int[] _nullComponent = {ID_FOR_NULL};

  public GroupingSetsGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext[] groupByExpressions, List<int[]> groupingSets, int numGroupsLimit,
      boolean nullHandlingEnabled) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _storedTypes = new DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];
    _isSingleValue = new boolean[_numGroupByExpressions];
    _nullHandlingEnabled = nullHandlingEnabled;
    _numGroupsLimit = numGroupsLimit;
    boolean hasMvColumn = false;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpressions[i]);
      _isSingleValue[i] = columnContext.isSingleValue();
      hasMvColumn |= !_isSingleValue[i];
      _storedTypes[i] = columnContext.getDataType().getStoredType();
      // Prefer the segment's native dictionary so dict-encoded values are resolved by dict-id lookup instead
      // of re-hashing every raw value into an on-the-fly dictionary. This mirrors the fast path in
      // NoDictionaryMultiColumnGroupKeyGenerator. Native dict-ids are >= 0 and so never collide with the
      // ID_FOR_NULL sentinel. Skip it when null handling is enabled (the raw forward index may not expose
      // dict-ids and NULLs must map to the sentinel) -- fall back to an on-the-fly dictionary in that case.
      Dictionary dictionary = nullHandlingEnabled || !columnContext.isDictionaryEncoded() ? null
          : columnContext.getDictionary();
      if (dictionary != null) {
        _dictionaries[i] = dictionary;
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
      }
    }
    _hasMvColumn = hasMvColumn;

    _numSets = groupingSets.size();
    _setContains = new boolean[_numSets][_numGroupByExpressions];
    for (int s = 0; s < _numSets; s++) {
      for (int columnIndex : groupingSets.get(s)) {
        _setContains[s][columnIndex] = true;
      }
    }

    // Decide whether the composite key fits in a long. This requires every union column to be dictionary-
    // encoded (so its id range is bounded by a known cardinality, reserving one extra value for the NULL /
    // rolled-up sentinel) and the total bit-width of all column ids plus the set ordinal to be <= 64.
    _bitShifts = new int[_numGroupByExpressions + 1];
    _nullPackedIds = new int[_numGroupByExpressions];
    boolean longPacking = true;
    int totalBits = 0;
    for (int i = 0; i < _numGroupByExpressions && longPacking; i++) {
      if (_dictionaries[i] == null) {
        longPacking = false;
        break;
      }
      // Reserve one id above the real dict-ids (0..cardinality-1) for NULL / rolled-up columns.
      int cardinality = _dictionaries[i].length();
      _nullPackedIds[i] = cardinality;
      _bitShifts[i] = totalBits;
      totalBits += bitsRequired(cardinality + 1L);
      if (totalBits > 64) {
        longPacking = false;
      }
    }
    if (longPacking) {
      _bitShifts[_numGroupByExpressions] = totalBits;
      totalBits += bitsRequired(_numSets);
      longPacking = totalBits <= 64;
    }
    _longPacking = longPacking;

    if (_longPacking) {
      _longGroupKeyMap = new Long2IntOpenHashMap();
      _longGroupKeyMap.defaultReturnValue(INVALID_ID);
      _groupKeyMap = null;
    } else {
      _groupKeyMap = new Object2IntOpenHashMap<>();
      _groupKeyMap.defaultReturnValue(INVALID_ID);
      _longGroupKeyMap = null;
    }
  }

  /// Number of bits needed to represent the values `0 .. numValues - 1` (0 for an empty range).
  private static int bitsRequired(long numValues) {
    return numValues <= 1 ? 0 : 64 - Long.numberOfLeadingZeros(numValues - 1);
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _numGroupsLimit;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    throw new UnsupportedOperationException(
        "GroupingSetsGroupKeyGenerator only supports the multi-value group key path");
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    if (_hasMvColumn) {
      generateKeysForBlockWithMv(valueBlock, groupKeys);
      return;
    }
    int numDocs = valueBlock.getNumDocs();
    /// Resolve each union column's value to an on-the-fly dictionary id per row (ID_FOR_NULL for nulls). The
    /// id of a value is independent of which grouping set uses it, so it is computed once per row here.
    int[][] columnIds = new int[_numGroupByExpressions][];
    for (int col = 0; col < _numGroupByExpressions; col++) {
      columnIds[col] = resolveColumnIds(valueBlock, col, numDocs);
    }
    if (_longPacking) {
      generateKeysForBlockSvLongPacked(numDocs, columnIds, groupKeys);
      return;
    }
    /// For each row, emit one group id per grouping set. Reuse a single key buffer (flyweight) across rows and
    /// sets, allocating a fresh one only when a new group is inserted (the map then owns that buffer). This
    /// mirrors NoDictionaryMultiColumnGroupKeyGenerator and avoids a per-(row, set) allocation.
    int[] keyValues = new int[_numGroupByExpressions + 1];
    FixedIntArray flyweightKey = new FixedIntArray(keyValues);
    for (int row = 0; row < numDocs; row++) {
      int[] ids = reuseOrAllocateRow(groupKeys, row);
      for (int s = 0; s < _numSets; s++) {
        int numGroups = _groupKeyMap.size();
        for (int col = 0; col < _numGroupByExpressions; col++) {
          keyValues[col] = _setContains[s][col] ? columnIds[col][row] : ID_FOR_NULL;
        }
        keyValues[_numGroupByExpressions] = s;
        int groupId = getGroupIdForKey(flyweightKey);
        if (groupId == numGroups) {
          /// A new group was inserted, so the map now retains this buffer; allocate a fresh one to reuse.
          keyValues = new int[_numGroupByExpressions + 1];
          flyweightKey = new FixedIntArray(keyValues);
        }
        ids[s] = groupId;
      }
      groupKeys[row] = ids;
    }
  }

  /// Single-value long-packed path: pack the composite key (per-column dict-ids plus the set ordinal) into a
  /// single `long` and resolve it through [#_longGroupKeyMap], avoiding any per-(row, set) key allocation.
  private void generateKeysForBlockSvLongPacked(int numDocs, int[][] columnIds, int[][] groupKeys) {
    for (int row = 0; row < numDocs; row++) {
      int[] ids = reuseOrAllocateRow(groupKeys, row);
      for (int s = 0; s < _numSets; s++) {
        boolean[] setContains = _setContains[s];
        // Pack the set ordinal into the trailing slot, then each column's id into its slot; a column rolled up
        // for this set contributes the reserved NULL id so it stays distinct from a participating real value.
        long key = ((long) s) << _bitShifts[_numGroupByExpressions];
        for (int col = 0; col < _numGroupByExpressions; col++) {
          int id = setContains[col] ? packedId(col, columnIds[col][row]) : _nullPackedIds[col];
          key |= ((long) id) << _bitShifts[col];
        }
        ids[s] = getGroupIdForLongKey(key);
      }
      groupKeys[row] = ids;
    }
  }

  /// Returns a per-row `int[_numSets]` buffer for `groupKeys[row]`, reusing the existing array from a prior
  /// block when it already has the right length. The single-value paths always overwrite all `_numSets` slots,
  /// and `groupKeys` (the executor's thread-local buffer) is not read across `generateKeysForBlock` calls, so
  /// reuse avoids a per-row allocation on every block. The MV path cannot use this (its row length varies with
  /// the Cartesian product), so it allocates fresh arrays.
  private int[] reuseOrAllocateRow(int[][] groupKeys, int row) {
    int[] existing = groupKeys[row];
    return existing != null && existing.length == _numSets ? existing : new int[_numSets];
  }

  /// Maps a resolved column id to its packed representation: real dict-ids (`>= 0`) pass through, while the
  /// NULL sentinel maps to the column's reserved id (its cardinality).
  private int packedId(int col, int id) {
    return id == ID_FOR_NULL ? _nullPackedIds[col] : id;
  }

  /// Multi-value path: a participating MV column contributes its full list of value ids, so a row expands
  /// over the Cartesian product of participating MV columns' values, composed with the per-set expansion.
  private void generateKeysForBlockWithMv(ValueBlock valueBlock, int[][] groupKeys) {
    int numDocs = valueBlock.getNumDocs();
    /// Per column, per row: the list of value ids (length 1 for a single-value column, length N for an MV
    /// column with N values in that row).
    int[][][] columnValueIds = new int[_numGroupByExpressions][][];
    for (int col = 0; col < _numGroupByExpressions; col++) {
      columnValueIds[col] = resolveColumnValueIds(valueBlock, col, numDocs);
    }
    int[][] keyComponents = new int[_numGroupByExpressions + 1][];
    int[] ordinalComponent = new int[1];
    keyComponents[_numGroupByExpressions] = ordinalComponent;
    for (int row = 0; row < numDocs; row++) {
      IntArrayList rowGroupIds = new IntArrayList(_numSets);
      for (int s = 0; s < _numSets; s++) {
        for (int col = 0; col < _numGroupByExpressions; col++) {
          keyComponents[col] = _setContains[s][col] ? columnValueIds[col][row] : _nullComponent;
        }
        ordinalComponent[0] = s;
        if (_longPacking) {
          expandGroupIdsLongPacked(keyComponents, ((long) s) << _bitShifts[_numGroupByExpressions], 0,
              rowGroupIds);
        } else {
          expandGroupIds(keyComponents, new int[_numGroupByExpressions + 1], 0, rowGroupIds);
        }
      }
      groupKeys[row] = rowGroupIds.toIntArray();
    }
  }

  /// Recursively builds composite keys from the per-column id lists (Cartesian product) and resolves each to
  /// a group id appended to `out`.
  private void expandGroupIds(int[][] keyComponents, int[] keyValues, int level, IntArrayList out) {
    if (level == keyComponents.length) {
      out.add(getGroupIdForKey(new FixedIntArray(keyValues.clone())));
      return;
    }
    for (int id : keyComponents[level]) {
      keyValues[level] = id;
      expandGroupIds(keyComponents, keyValues, level + 1, out);
    }
  }

  /// Long-packed variant of [#expandGroupIds]: folds each column's id into the running packed `long` key
  /// (Cartesian product across MV columns) and resolves each completed key to a group id. The set ordinal is
  /// already folded into `keySoFar` by the caller, so only the union columns (`level < _numGroupByExpressions`)
  /// are expanded here.
  private void expandGroupIdsLongPacked(int[][] keyComponents, long keySoFar, int level, IntArrayList out) {
    if (level == _numGroupByExpressions) {
      out.add(getGroupIdForLongKey(keySoFar));
      return;
    }
    int shift = _bitShifts[level];
    for (int id : keyComponents[level]) {
      long packed = id == ID_FOR_NULL ? _nullPackedIds[level] : id;
      expandGroupIdsLongPacked(keyComponents, keySoFar | (packed << shift), level + 1, out);
    }
  }

  /// Resolves a column's value ids per row as a list (length 1 for single-value columns, the MV value list
  /// for multi-value columns).
  private int[][] resolveColumnValueIds(ValueBlock valueBlock, int col, int numDocs) {
    int[][] ids = new int[numDocs][];
    if (_isSingleValue[col]) {
      int[] svIds = resolveColumnIds(valueBlock, col, numDocs);
      for (int row = 0; row < numDocs; row++) {
        ids[row] = new int[]{svIds[row]};
      }
      return ids;
    }
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[col]);
    // Fast path: dict-encoded MV column resolves to native dict-id lists directly.
    if (_dictionaries[col] != null) {
      return blockValSet.getDictionaryIdsMV();
    }
    ValueToIdMap dictionary = _onTheFlyDictionaries[col];
    switch (_storedTypes[col]) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int row = 0; row < numDocs; row++) {
          int[] values = intValues[row];
          int[] rowIds = new int[values.length];
          for (int k = 0; k < values.length; k++) {
            rowIds[k] = dictionary.put(values[k]);
          }
          ids[row] = rowIds;
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int row = 0; row < numDocs; row++) {
          long[] values = longValues[row];
          int[] rowIds = new int[values.length];
          for (int k = 0; k < values.length; k++) {
            rowIds[k] = dictionary.put(values[k]);
          }
          ids[row] = rowIds;
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int row = 0; row < numDocs; row++) {
          float[] values = floatValues[row];
          int[] rowIds = new int[values.length];
          for (int k = 0; k < values.length; k++) {
            rowIds[k] = dictionary.put(values[k]);
          }
          ids[row] = rowIds;
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int row = 0; row < numDocs; row++) {
          double[] values = doubleValues[row];
          int[] rowIds = new int[values.length];
          for (int k = 0; k < values.length; k++) {
            rowIds[k] = dictionary.put(values[k]);
          }
          ids[row] = rowIds;
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int row = 0; row < numDocs; row++) {
          String[] values = stringValues[row];
          int[] rowIds = new int[values.length];
          for (int k = 0; k < values.length; k++) {
            rowIds[k] = dictionary.put(values[k]);
          }
          ids[row] = rowIds;
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Illegal multi-value data type for grouping-sets group key generator: " + _storedTypes[col]);
    }
    if (_nullHandlingEnabled) {
      // A null row's values were resolved above and are discarded here. Only the composed key decides a group, so the
      // ids they took in the on-the-fly dictionary go unused rather than becoming groups of their own.
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        PeekableIntIterator nullIterator = nullBitmap.getIntIterator();
        while (nullIterator.hasNext()) {
          ids[nullIterator.next()] = _nullComponent;
        }
      }
    }
    return ids;
  }

  /// Resolves the on-the-fly dictionary id for the given union column across all rows in the block, mapping
  /// null values (when null handling is enabled) to [#ID_FOR_NULL].
  private int[] resolveColumnIds(ValueBlock valueBlock, int col, int numDocs) {
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[col]);
    // Fast path: a dictionary-encoded column resolves to the segment's native dict-ids directly, skipping the
    // per-value hash into an on-the-fly dictionary. Returned array is read-only for this generator.
    if (_dictionaries[col] != null) {
      return blockValSet.getDictionaryIdsSV();
    }
    int[] ids = new int[numDocs];
    ValueToIdMap dictionary = _onTheFlyDictionaries[col];
    RoaringBitmap nullBitmap = _nullHandlingEnabled ? blockValSet.getNullBitmap() : null;
    if (nullBitmap != null && nullBitmap.isEmpty()) {
      nullBitmap = null;
    }
    switch (_storedTypes[col]) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(intValues[row]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(longValues[row]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(floatValues[row]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(doubleValues[row]);
        }
        break;
      case BIG_DECIMAL:
        Object[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(bigDecimalValues[row]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(stringValues[row]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int row = 0; row < numDocs; row++) {
          ids[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictionary.put(new ByteArray(bytesValues[row]));
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Illegal data type for grouping-sets group key generator: " + _storedTypes[col]);
    }
    return ids;
  }

  private static boolean isNull(RoaringBitmap nullBitmap, int row) {
    return nullBitmap != null && nullBitmap.contains(row);
  }

  /// Returns the group id for the given composite key, creating a new group while the per-segment group
  /// limit has not been reached. Once the limit is reached, only existing groups are returned and brand-new
  /// keys map to [#INVALID_ID] (the aggregation result holders skip `INVALID_ID`).
  private int getGroupIdForKey(FixedIntArray keyList) {
    int numGroups = _groupKeyMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupKeyMap.computeIfAbsent(keyList, k -> numGroups);
    } else {
      return _groupKeyMap.getInt(keyList);
    }
  }

  /// Long-packed counterpart of [#getGroupIdForKey]: resolves a packed composite key to a group id, creating
  /// a new group while under the per-segment group limit and returning [#INVALID_ID] for brand-new keys once
  /// the limit is reached.
  private int getGroupIdForLongKey(long key) {
    int numGroups = _longGroupKeyMap.size();
    if (numGroups < _numGroupsLimit) {
      int id = _longGroupKeyMap.putIfAbsent(key, numGroups);
      return id == INVALID_ID ? numGroups : id;
    } else {
      return _longGroupKeyMap.get(key);
    }
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _longPacking ? _longGroupKeyMap.size() : _groupKeyMap.size();
  }

  @Override
  public int getNumKeys() {
    return _longPacking ? _longGroupKeyMap.size() : _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return _longPacking ? new LongGroupKeyIterator() : new GroupKeyIterator();
  }

  /// Reconstructs the output row for a composite key: the union column values (NULL where rolled up) followed
  /// by the integer grouping-set-ordinal discriminator.
  private Object[] buildKeysFromIds(FixedIntArray keyList) {
    int[] ids = keyList.elements();
    Object[] keys = new Object[_numGroupByExpressions + 1];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (ids[i] == ID_FOR_NULL) {
        keys[i] = null;
      } else if (_dictionaries[i] != null) {
        keys[i] = _dictionaries[i].getInternal(ids[i]);
      } else {
        keys[i] = _onTheFlyDictionaries[i].get(ids[i]);
      }
    }
    /// The trailing slot stores the grouping-set ordinal directly (not a dictionary id).
    keys[_numGroupByExpressions] = ids[_numGroupByExpressions];
    return keys;
  }

  /// Unpacks a long-packed composite key into the output row: each union column value (NULL where the packed
  /// id is the reserved sentinel) followed by the integer grouping-set-ordinal discriminator. Inverse of the
  /// packing done in [#generateKeysForBlockSvLongPacked] / [#expandGroupIdsLongPacked].
  private Object[] buildKeysFromLong(long key) {
    Object[] keys = new Object[_numGroupByExpressions + 1];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      int packedId = extractPackedId(key, i);
      keys[i] = packedId == _nullPackedIds[i] ? null : _dictionaries[i].getInternal(packedId);
    }
    keys[_numGroupByExpressions] = (int) (key >>> _bitShifts[_numGroupByExpressions]);
    return keys;
  }

  /// Extracts union column `col`'s packed id from a long-packed composite key.
  private int extractPackedId(long key, int col) {
    int nextShift = col + 1 < _numGroupByExpressions ? _bitShifts[col + 1] : _bitShifts[_numGroupByExpressions];
    long mask = (1L << (nextShift - _bitShifts[col])) - 1;
    return (int) ((key >>> _bitShifts[col]) & mask);
  }

  private class GroupKeyIterator implements Iterator<GroupKey> {
    private final ObjectIterator<Object2IntMap.Entry<FixedIntArray>> _iterator;
    private final GroupKey _groupKey;

    GroupKeyIterator() {
      _iterator = _groupKeyMap.object2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Object2IntMap.Entry<FixedIntArray> entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = buildKeysFromIds(entry.getKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /// Long-packed counterpart of [GroupKeyIterator]: iterates [#_longGroupKeyMap] and unpacks each key.
  private class LongGroupKeyIterator implements Iterator<GroupKey> {
    private final ObjectIterator<Long2IntMap.Entry> _iterator;
    private final GroupKey _groupKey;

    LongGroupKeyIterator() {
      _iterator = _longGroupKeyMap.long2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Long2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = buildKeysFromLong(entry.getLongKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
