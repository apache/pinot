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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.GroupingSets;
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
import org.roaringbitmap.RoaringBitmap;


/**
 * {@link GroupKeyGenerator} for SQL {@code GROUPING SETS} / {@code ROLLUP} / {@code CUBE}.
 *
 * <p>Each input row contributes to one group per grouping set, so the generator produces <b>K group ids per row</b>
 * (K = number of sets) through the multi-valued {@link #generateKeysForBlock(ValueBlock, int[][])} path — the existing
 * {@code aggregateGroupByMV} machinery then adds the row's value to each of those groups exactly once, which is the
 * grouping-set semantics.
 *
 * <p>The internal key for set {@code k} is a {@link FixedIntArray} of width {@code N + 1}: positions {@code 0..N-1}
 * hold the participating column's value id ({@code ID_FOR_ROLLUP} for columns rolled up in that set, and
 * {@code ID_FOR_NULL} for a real NULL), and position {@code N} holds the set's {@code $groupingId} value (the
 * {@code GROUPING_ID} over all union columns). That last column both keeps different sets from colliding (so a
 * rolled-up NULL never merges with a real-data NULL) and powers {@code GROUPING()} / {@code GROUPING_ID()}.
 * {@link #getGroupKeys()} decodes each key into an {@code Object[N + 1]}: rolled-up / NULL columns become
 * {@code null} and the last element is the {@code $groupingId} INT value.
 *
 * <p>v1 limitations / follow-ups: multi-valued group-by columns are not supported here (rejected in the constructor);
 * group trimming is not yet implemented; the per-row/per-set key buffers are freshly allocated for clarity rather than
 * reusing a flyweight {@link FixedIntArray} (allocate-on-new-group only) the way
 * {@link NoDictionaryMultiColumnGroupKeyGenerator} does; and the per-stored-type value-id resolution duplicates that
 * class. The allocation reuse and the dedup refactor are intended once end-to-end tests cover this path.
 */
public class GroupingSetsGroupKeyGenerator implements GroupKeyGenerator {
  // Sentinel value ids stored in the FixedIntArray key. ID_FOR_NULL marks a real NULL in a participating column;
  // ID_FOR_ROLLUP marks a column that is rolled up (not in the set). Both decode to a null output value, but they are
  // distinct ids so they never collide, and the trailing $groupingId column separates sets regardless.
  private static final int ID_FOR_NULL = INVALID_ID - 1;
  private static final int ID_FOR_ROLLUP = INVALID_ID - 2;

  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int _numKeyColumns;
  private final DataType[] _storedTypes;
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final int[] _masks;
  private final int[] _groupingIdValues;
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  private final int _numGroupsLimit;
  private final boolean _nullHandlingEnabled;

  public GroupingSetsGroupKeyGenerator(BaseProjectOperator<?> projectOperator, ExpressionContext[] groupByExpressions,
      int[] groupingSets, int numGroupsLimit, boolean nullHandlingEnabled) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _numKeyColumns = _numGroupByExpressions + 1;
    _storedTypes = new DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];
    _nullHandlingEnabled = nullHandlingEnabled;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpressions[i]);
      // Multi-valued group-by columns are not supported in grouping sets (a row would expand both by MV cardinality
      // and by grouping set); reject explicitly rather than mis-reading the MV column through single-valued accessors.
      Preconditions.checkState(columnContext.isSingleValue(),
          "GROUPING SETS / ROLLUP / CUBE does not support multi-valued group-by column: %s", groupByExpressions[i]);
      _storedTypes[i] = columnContext.getDataType().getStoredType();
      // Use the column dictionary only when the forward index is genuinely dict-encoded and null handling is off
      // (mirrors NoDictionaryMultiColumnGroupKeyGenerator); otherwise build an on-the-fly dictionary over raw values.
      Dictionary dictionary =
          _nullHandlingEnabled || !columnContext.isDictionaryEncoded() ? null : columnContext.getDictionary();
      if (dictionary != null) {
        _dictionaries[i] = dictionary;
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
      }
    }
    _masks = groupingSets;
    _groupingIdValues = new int[groupingSets.length];
    for (int k = 0; k < groupingSets.length; k++) {
      _groupingIdValues[k] = GroupingSets.groupingIdValue(groupingSets[k], _numGroupByExpressions);
    }
    _groupKeyMap = new Object2IntOpenHashMap<>();
    _groupKeyMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _numGroupsLimit;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    throw new UnsupportedOperationException("Grouping-set group keys are multi-valued (one row maps to one group per "
        + "set); use generateKeysForBlock(ValueBlock, int[][])");
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    int numDocs = valueBlock.getNumDocs();
    // Resolve each group-by column to a per-row value id once for the whole block.
    int[][] columnValueIds = new int[_numGroupByExpressions][];
    for (int col = 0; col < _numGroupByExpressions; col++) {
      columnValueIds[col] = resolveColumnValueIds(valueBlock, col, numDocs);
    }
    int numSets = _masks.length;
    for (int row = 0; row < numDocs; row++) {
      IntArrayList rowGroupIds = new IntArrayList(numSets);
      for (int k = 0; k < numSets; k++) {
        int mask = _masks[k];
        int[] keyValues = new int[_numKeyColumns];
        for (int col = 0; col < _numGroupByExpressions; col++) {
          keyValues[col] = GroupingSets.participates(mask, col) ? columnValueIds[col][row] : ID_FOR_ROLLUP;
        }
        keyValues[_numGroupByExpressions] = _groupingIdValues[k];
        int groupId = getGroupIdForKey(new FixedIntArray(keyValues));
        if (groupId != INVALID_ID) {
          rowGroupIds.add(groupId);
        }
      }
      groupKeys[row] = rowGroupIds.toIntArray();
    }
  }

  /** Resolves a group-by column to its per-row value id (dictionary id, on-the-fly id, or {@code ID_FOR_NULL}). */
  private int[] resolveColumnValueIds(ValueBlock valueBlock, int col, int numDocs) {
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[col]);
    RoaringBitmap nullBitmap = _nullHandlingEnabled ? blockValSet.getNullBitmap() : null;
    int[] valueIds = new int[numDocs];
    if (_dictionaries[col] != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int row = 0; row < numDocs; row++) {
        valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : dictIds[row];
      }
      return valueIds;
    }
    ValueToIdMap onTheFlyDictionary = _onTheFlyDictionaries[col];
    switch (_storedTypes[col]) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(intValues[row]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(longValues[row]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(floatValues[row]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(doubleValues[row]);
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(bigDecimalValues[row]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] = isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(stringValues[row]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int row = 0; row < numDocs; row++) {
          valueIds[row] =
              isNull(nullBitmap, row) ? ID_FOR_NULL : onTheFlyDictionary.put(new ByteArray(bytesValues[row]));
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal data type for grouping-set group-by column: " + _storedTypes[col]);
    }
    return valueIds;
  }

  private static boolean isNull(RoaringBitmap nullBitmap, int row) {
    return nullBitmap != null && nullBitmap.contains(row);
  }

  private int getGroupIdForKey(FixedIntArray key) {
    int numGroups = _groupKeyMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupKeyMap.computeIfAbsent(key, k -> numGroups);
    }
    return _groupKeyMap.getInt(key);
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

  private Object[] buildKeysFromIds(FixedIntArray key) {
    int[] valueIds = key.elements();
    Object[] keys = new Object[_numKeyColumns];
    for (int col = 0; col < _numGroupByExpressions; col++) {
      int valueId = valueIds[col];
      if (valueId == ID_FOR_NULL || valueId == ID_FOR_ROLLUP) {
        keys[col] = null;
      } else if (_dictionaries[col] != null) {
        keys[col] = _dictionaries[col].getInternal(valueId);
      } else {
        keys[col] = _onTheFlyDictionaries[col].get(valueId);
      }
    }
    // The trailing $groupingId column is the raw value, surfaced as an INT.
    keys[_numGroupByExpressions] = valueIds[_numGroupByExpressions];
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
