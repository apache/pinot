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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.roaringbitmap.RoaringBitmap;


/// {@link GroupKeyGenerator} for GROUP BY GROUPING SETS / ROLLUP / CUBE queries in the single-stage engine.
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
/// trailing `groupingId` is a bitmask over the union with bit `i` set iff column `i` is rolled up in `S`
/// (PostgreSQL `GROUPING` semantics: 1 = aggregated away). The `groupingId` is appended as a synthetic key
/// column so that rows from different grouping sets never collide -- e.g. set `{a}` with `b` rolled up to
/// NULL stays distinct from set `{a, b}` where `b` is genuinely NULL, even though both render `(a, NULL)`.
///
/// This generator always emits multiple keys per row, so it is only used via the multi-value
/// ({@code int[][]}) executor path. The union columns are resolved through per-column on-the-fly
/// dictionaries (like {@link NoDictionaryMultiColumnGroupKeyGenerator}) so that NULL keys -- which grouping
/// sets produce regardless of the query's null-handling option -- are representable and reconstructable.
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
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final boolean[] _isSingleValue;
  private final boolean _hasMvColumn;
  private final boolean _nullHandlingEnabled;
  private final int _numGroupsLimit;

  /// Per grouping set: membership over the union columns, and the grouping-id bitmask (bit i set iff column i
  /// is rolled up / excluded from the set). The bitmask is also the value stored in the synthetic
  /// $groupingId key column and consumed by GROUPING() / GROUPING_ID().
  private final boolean[][] _setContains;
  private final int[] _setBitmasks;
  private final int _numSets;

  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  /// Reusable single-element id list for a rolled-up (non-participating) column on the MV path. Per-instance
  /// (segment-confined, never shared across query threads) and treated as read-only: expandGroupIds copies its
  /// element into the composite key and never mutates it.
  private final int[] _nullComponent = {ID_FOR_NULL};

  public GroupingSetsGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext[] groupByExpressions, List<int[]> groupingSets, int numGroupsLimit,
      boolean nullHandlingEnabled) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _storedTypes = new DataType[_numGroupByExpressions];
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
      _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
    }
    _hasMvColumn = hasMvColumn;

    _numSets = groupingSets.size();
    _setContains = new boolean[_numSets][_numGroupByExpressions];
    _setBitmasks = new int[_numSets];
    for (int s = 0; s < _numSets; s++) {
      for (int columnIndex : groupingSets.get(s)) {
        _setContains[s][columnIndex] = true;
      }
      int bitmask = 0;
      for (int i = 0; i < _numGroupByExpressions; i++) {
        if (!_setContains[s][i]) {
          bitmask |= 1 << i;
        }
      }
      _setBitmasks[s] = bitmask;
    }

    _groupKeyMap = new Object2IntOpenHashMap<>();
    _groupKeyMap.defaultReturnValue(INVALID_ID);
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
    /// For each row, emit one group id per grouping set. Reuse a single key buffer (flyweight) across rows and
    /// sets, allocating a fresh one only when a new group is inserted (the map then owns that buffer). This
    /// mirrors NoDictionaryMultiColumnGroupKeyGenerator and avoids a per-(row, set) allocation.
    int[] keyValues = new int[_numGroupByExpressions + 1];
    FixedIntArray flyweightKey = new FixedIntArray(keyValues);
    for (int row = 0; row < numDocs; row++) {
      int[] ids = new int[_numSets];
      for (int s = 0; s < _numSets; s++) {
        int numGroups = _groupKeyMap.size();
        for (int col = 0; col < _numGroupByExpressions; col++) {
          keyValues[col] = _setContains[s][col] ? columnIds[col][row] : ID_FOR_NULL;
        }
        keyValues[_numGroupByExpressions] = _setBitmasks[s];
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
    int[] bitmaskComponent = new int[1];
    keyComponents[_numGroupByExpressions] = bitmaskComponent;
    for (int row = 0; row < numDocs; row++) {
      IntArrayList rowGroupIds = new IntArrayList(_numSets);
      for (int s = 0; s < _numSets; s++) {
        for (int col = 0; col < _numGroupByExpressions; col++) {
          keyComponents[col] = _setContains[s][col] ? columnValueIds[col][row] : _nullComponent;
        }
        bitmaskComponent[0] = _setBitmasks[s];
        expandGroupIds(keyComponents, new int[_numGroupByExpressions + 1], 0, rowGroupIds);
      }
      groupKeys[row] = rowGroupIds.toIntArray();
    }
  }

  /// Recursively builds composite keys from the per-column id lists (Cartesian product) and resolves each to
  /// a group id appended to {@code out}.
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
    return ids;
  }

  /// Resolves the on-the-fly dictionary id for the given union column across all rows in the block, mapping
  /// null values (when null handling is enabled) to {@link #ID_FOR_NULL}.
  private int[] resolveColumnIds(ValueBlock valueBlock, int col, int numDocs) {
    int[] ids = new int[numDocs];
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[col]);
    ValueToIdMap dictionary = _onTheFlyDictionaries[col];
    RoaringBitmap nullBitmap = _nullHandlingEnabled ? blockValSet.getNullBitmap() : null;
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
  /// keys map to {@link #INVALID_ID} (the aggregation result holders skip {@code INVALID_ID}).
  private int getGroupIdForKey(FixedIntArray keyList) {
    int numGroups = _groupKeyMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupKeyMap.computeIfAbsent(keyList, k -> numGroups);
    } else {
      return _groupKeyMap.getInt(keyList);
    }
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public int getNumKeys() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return new GroupKeyIterator();
  }

  /// Reconstructs the output row for a composite key: the union column values (NULL where rolled up) followed
  /// by the integer grouping-id discriminator.
  private Object[] buildKeysFromIds(FixedIntArray keyList) {
    int[] ids = keyList.elements();
    Object[] keys = new Object[_numGroupByExpressions + 1];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      keys[i] = ids[i] == ID_FOR_NULL ? null : _onTheFlyDictionaries[i].get(ids[i]);
    }
    /// The trailing slot stores the grouping-id bitmask directly (not a dictionary id).
    keys[_numGroupByExpressions] = ids[_numGroupByExpressions];
    return keys;
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
}
