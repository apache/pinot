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

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Class for generating group keys (groupId-stringKey pair) for a given list of dictionary encoded group-by columns.
///
/// The maximum number of possible group keys is the cardinality product of all the group-by columns.
///
/// The raw key is generated from the dictionary ids of the group-by columns.
///
/// With null handling enabled, a column that can produce a null reserves one further dictionary id, one past its
/// last real id, to stand for it, so its cardinality counts one more than the dictionary holds. A null then composes
/// into the raw key like any other value and is read back out as SQL `NULL`. A column read from a segment that tracks
/// no nulls reserves nothing, and neither does any column when null handling is disabled, which leaves the
/// cardinalities, the raw key arithmetic and the block reads exactly as they are without this.
///
/// - If the maximum number of possible group keys is less than a threshold (10K), directly use the raw key as the
///   group id. (ARRAY_BASED)
/// - If the maximum number of possible group keys is larger than the threshold, but still fit into integer, generate
///   integer raw keys and map them onto contiguous group ids. (INT_MAP_BASED)
/// - If the maximum number of possible group keys cannot fit into integer, but still fit into long, generate long
///   raw keys and map them onto contiguous group ids. (LONG_MAP_BASED)
/// - If the maximum number of possible group keys cannot fit into long, use int arrays as the raw keys to store the
///   dictionary ids of all the group-by columns and map them onto contiguous group ids. (ARRAY_MAP_BASED)
///
/// All the logic is maintained internally, and to the outside world, the group ids are always int type, and are
/// bounded by the number of groups limit (globalGroupIdUpperBound is always smaller or equal to numGroupsLimit).
///
/// When IN/EQ predicates on the group-by expressions prove an upper bound of the number of distinct groups smaller
/// than the cardinality product (see optimizeMaxInitialResultHolderCapacity), the bound shrinks
/// globalGroupIdUpperBound but never the raw key space, so a map based holder is used even when the bound is below
/// the ARRAY_BASED threshold.
public class DictionaryBasedGroupKeyGenerator implements GroupKeyGenerator {
  // NOTE: map size = map capacity (power of 2) * load factor
  private static final int INITIAL_MAP_SIZE = (int) ((1 << 9) * 0.75f);
  private static final int MAX_CACHING_MAP_SIZE = (int) ((1 << 20) * 0.75f);
  private static final int MAX_DICTIONARY_INTERN_TABLE_SIZE = 10000;

  @VisibleForTesting
  static final ThreadLocal<IntGroupIdMap> THREAD_LOCAL_INT_MAP = ThreadLocal.withInitial(IntGroupIdMap::new);
  @VisibleForTesting
  static final ThreadLocal<Long2IntOpenHashMap> THREAD_LOCAL_LONG_MAP = ThreadLocal.withInitial(() -> {
    Long2IntOpenHashMap map = new Long2IntOpenHashMap(INITIAL_MAP_SIZE);
    map.defaultReturnValue(INVALID_ID);
    return map;
  });
  @VisibleForTesting
  static final ThreadLocal<Object2IntOpenHashMap<IntArray>> THREAD_LOCAL_INT_ARRAY_MAP = ThreadLocal.withInitial(() -> {
    Object2IntOpenHashMap<IntArray> map = new Object2IntOpenHashMap<>(INITIAL_MAP_SIZE);
    map.defaultReturnValue(INVALID_ID);
    return map;
  });

  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int[] _cardinalities;
  private final boolean[] _isSingleValueColumn;
  private final Dictionary[] _dictionaries;
  // Dictionary id standing for a null value in each group-by column, or -1 for a column that produces none. Reserving
  // the id one past the column's last real id is what keeps nulls apart from the column's default null value, which is
  // the value a null row is physically stored as. Null when no column reserves one at all.
  @Nullable
  private final int[] _nullDictIds;
  // The first dimension is the index of group-by column
  // Reusable buffer for single-value column dictionary ids
  private final int[][] _singleValueDictIds;
  // Reusable buffers holding a block's dictionary ids with the null rows rewritten, filled in on the first block of
  // a column that actually contains nulls, and null whenever no column reserves a null dictionary id
  @Nullable
  private final int[][] _nullReplacedSingleValueDictIds;
  // Reusable buffer for multi-value column dictionary ids
  private final int[][][] _multiValueDictIds;
  @Nullable
  private final int[][][] _nullReplacedMultiValueDictIds;

  private final Object[][] _internedDictionaryValues;

  private final int _globalGroupIdUpperBound;
  private final RawKeyHolder _rawKeyHolder;

  /// @param groupByExpressionSizesFromPredicates IN/EQ predicate sizes for the group-by expressions, used to bound
  ///        the number of distinct groups. Sizes for multi-value expressions are ignored: every value inside a
  ///        matching row becomes a group, so the predicate size does not bound the group count.
  public DictionaryBasedGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext[] groupByExpressions, int numGroupsLimit, int arrayBasedThreshold,
      boolean nullHandlingEnabled, @Nullable Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;

    _cardinalities = new int[_numGroupByExpressions];
    _isSingleValueColumn = new boolean[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _singleValueDictIds = new int[_numGroupByExpressions][];
    _multiValueDictIds = new int[_numGroupByExpressions][][];
    int[] nullDictIds = nullHandlingEnabled ? new int[_numGroupByExpressions] : null;
    boolean anyNullDictId = false;
    // no need to intern dictionary values when there is only one group by expression because
    // only one call will be made to the dictionary to extract each raw value.
    _internedDictionaryValues = _numGroupByExpressions > 1 ? new Object[_numGroupByExpressions][] : null;
    long cardinalityProduct = 1L;
    boolean longOverflow = false;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
      _dictionaries[i] = columnContext.getDictionary();
      assert _dictionaries[i] != null;
      int cardinality = _dictionaries[i].length();
      if (nullDictIds != null) {
        // Reserve an id unless the column is known to produce no null. A column read straight from a segment says so
        // through its null value vector, which is the same vector a query reads nulls from. A transform exposes no
        // data source to consult and can still hand out a null bitmap of its own, so it always reserves one.
        //
        // This decides nullability once, from one project operator, while a shared generator can be handed blocks
        // from others: FilteredGroupByOperator builds a single generator for every filtered aggregation. Those
        // operators must therefore agree on which group-by columns can produce a null. They do today, because a
        // star-tree is only chosen for a null handling query when no column it touches holds a null.
        DataSource dataSource = columnContext.getDataSource();
        boolean tracksNulls = dataSource == null || dataSource.getNullValueVector() != null;
        if (tracksNulls) {
          nullDictIds[i] = cardinality++;
          anyNullDictId = true;
        } else {
          nullDictIds[i] = -1;
        }
      }
      _cardinalities[i] = cardinality;
      if (_internedDictionaryValues != null && cardinality < MAX_DICTIONARY_INTERN_TABLE_SIZE) {
        _internedDictionaryValues[i] = new Object[cardinality];
      }
      if (!longOverflow) {
        if (cardinalityProduct > Long.MAX_VALUE / cardinality) {
          longOverflow = true;
        } else {
          cardinalityProduct *= cardinality;
        }
      }
      _isSingleValueColumn[i] = columnContext.isSingleValue();
    }
    if (anyNullDictId) {
      _nullDictIds = nullDictIds;
      _nullReplacedSingleValueDictIds = new int[_numGroupByExpressions][];
      _nullReplacedMultiValueDictIds = new int[_numGroupByExpressions][][];
    } else {
      _nullDictIds = null;
      _nullReplacedSingleValueDictIds = null;
      _nullReplacedMultiValueDictIds = null;
    }
    // An IN/EQ predicate on a group-by column bounds the number of distinct groups, so it can shrink the group id
    // upper bound (and thus the result holder sizes). It must not influence the holder type selection though: raw
    // keys are mixed-radix products over the FULL dictionary cardinalities regardless of the filter, so the
    // int/long/array-map decision (raw key range) has to be based on the full cardinality product.
    long optimizedGroupCountUpperBound = groupByExpressionSizesFromPredicates != null
        ? getOptimizedGroupByCardinality(groupByExpressionSizesFromPredicates) : Long.MAX_VALUE;
    int cappedNumGroupsLimit = (int) Math.min(numGroupsLimit, optimizedGroupCountUpperBound);
    // NOTE: We need to clean up the thread-local map before using it in case RawKeyHolder.close() is not called
    //       for the previous segment
    // TODO: Ensure RawKeyHolder.close()
    if (longOverflow) {
      // ArrayMapBasedHolder
      _globalGroupIdUpperBound = cappedNumGroupsLimit;
      Object2IntOpenHashMap<IntArray> groupIdMap = THREAD_LOCAL_INT_ARRAY_MAP.get();
      clearAndTrim(groupIdMap);
      _rawKeyHolder = new ArrayMapBasedHolder(groupIdMap);
    } else {
      if (cardinalityProduct > Integer.MAX_VALUE) {
        // LongMapBasedHolder
        _globalGroupIdUpperBound = cappedNumGroupsLimit;
        Long2IntOpenHashMap groupIdMap = THREAD_LOCAL_LONG_MAP.get();
        clearAndTrim(groupIdMap);
        _rawKeyHolder = new LongMapBasedHolder(groupIdMap);
      } else {
        _globalGroupIdUpperBound = (int) Math.min(cardinalityProduct, cappedNumGroupsLimit);
        // arrayBaseHolder fails with ArrayIndexOutOfBoundsException if numGroupsLimit < cardinalityProduct
        // because array doesn't fit all (potentially unsorted) values.
        // ArrayBasedHolder uses the raw key directly as the group id, so it is only valid when the group id space
        // covers the full cardinality product; when the predicate-based optimization proves a smaller upper bound,
        // fall back to IntMapBasedHolder which maps the sparse raw keys onto dense group ids. This deliberately
        // trades a hash lookup per row for smaller result holders, which is what the opt-in
        // optimizeMaxInitialResultHolderCapacity query option asks for.
        if (cardinalityProduct > arrayBasedThreshold || numGroupsLimit < cardinalityProduct
            || optimizedGroupCountUpperBound < cardinalityProduct) {
          // IntMapBasedHolder
          IntGroupIdMap groupIdMap = THREAD_LOCAL_INT_MAP.get();
          groupIdMap.clearAndTrim();
          _rawKeyHolder = new IntMapBasedHolder(groupIdMap);
        } else {
          _rawKeyHolder = new ArrayBasedHolder();
        }
      }
    }
  }

  /// Returns the upper bound of the number of distinct groups derived from the IN/EQ predicate sizes on the group-by
  /// expressions, or [Long#MAX_VALUE] if the bound overflows long (i.e. no usable bound). Multi-value expressions
  /// always contribute their full cardinality: every value inside a matching row becomes a group, so the predicate
  /// size does not bound their group count. Iterating the expression array (rather than a deduplicated map) keeps
  /// the bound comparable to the cardinality product when the same expression appears multiple times.
  private long getOptimizedGroupByCardinality(Map<ExpressionContext, Integer> groupByExpressionSizes) {
    long groupCountUpperBound = 1L;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      Integer size = _isSingleValueColumn[i] ? groupByExpressionSizes.get(_groupByExpressions[i]) : null;
      int minSize = size != null ? Math.min(size, _cardinalities[i]) : _cardinalities[i];
      if (minSize <= 0 || groupCountUpperBound > Long.MAX_VALUE / minSize) {
        return Long.MAX_VALUE;
      }
      groupCountUpperBound *= minSize;
    }
    return groupCountUpperBound;
  }

  private static void clearAndTrim(Long2IntOpenHashMap map) {
    int size = map.size();
    if (size > 0) {
      map.clear();
      if (size > MAX_CACHING_MAP_SIZE) {
        map.trim();
      }
    }
  }

  private static void clearAndTrim(Object2IntOpenHashMap<IntArray> map) {
    int size = map.size();
    if (size > 0) {
      map.clear();
      if (size > MAX_CACHING_MAP_SIZE) {
        map.trim();
      }
    }
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    // Fetch dictionary ids in the given block for all group-by columns
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      _singleValueDictIds[i] = getSingleValueDictIds(valueBlock.getBlockValueSet(_groupByExpressions[i]), i, numDocs);
    }
    _rawKeyHolder.processSingleValue(numDocs, groupKeys);
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    // Fetch dictionary ids in the given block for all group-by columns
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValueSet = valueBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_isSingleValueColumn[i]) {
        _singleValueDictIds[i] = getSingleValueDictIds(blockValueSet, i, numDocs);
      } else {
        _multiValueDictIds[i] = getMultiValueDictIds(blockValueSet, i, numDocs);
      }
    }
    _rawKeyHolder.processMultiValue(numDocs, groupKeys);
  }

  /// Returns the block's single-value dictionary ids, with every null row moved onto the column's reserved null
  /// dictionary id so that nulls form one group of their own.
  ///
  /// The block owns the array it hands out and shares it with everything else reading that column in this block, so
  /// the ids are copied before being rewritten rather than replaced in place. Returns the block's own array untouched
  /// when the column reserves no null dictionary id or the block holds no null.
  private int[] getSingleValueDictIds(BlockValSet blockValSet, int index, int numDocs) {
    int[] dictIds = blockValSet.getDictionaryIdsSV();
    if (_nullDictIds == null || _nullDictIds[index] < 0) {
      return dictIds;
    }
    int nullDictId = _nullDictIds[index];
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    if (nullBitmap == null || nullBitmap.isEmpty()) {
      return dictIds;
    }
    assert _nullReplacedSingleValueDictIds != null;
    int[] nullReplacedDictIds = _nullReplacedSingleValueDictIds[index];
    if (nullReplacedDictIds == null || nullReplacedDictIds.length < numDocs) {
      nullReplacedDictIds = new int[numDocs];
      _nullReplacedSingleValueDictIds[index] = nullReplacedDictIds;
    }
    System.arraycopy(dictIds, 0, nullReplacedDictIds, 0, numDocs);
    PeekableIntIterator nullIterator = nullBitmap.getIntIterator();
    while (nullIterator.hasNext()) {
      nullReplacedDictIds[nullIterator.next()] = nullDictId;
    }
    return nullReplacedDictIds;
  }

  /// Returns the block's multi-value dictionary ids, with every null row replaced by a single entry holding the
  /// column's reserved null dictionary id, so the row contributes one null group instead of being read as the
  /// column's default null value.
  ///
  /// Only the outer array is copied: the rows that are not null are handed on pointing at the arrays the block
  /// already gave out. Returns the block's own array untouched when the column reserves no null dictionary id or the
  /// block holds no null.
  private int[][] getMultiValueDictIds(BlockValSet blockValSet, int index, int numDocs) {
    int[][] dictIds = blockValSet.getDictionaryIdsMV();
    if (_nullDictIds == null || _nullDictIds[index] < 0) {
      return dictIds;
    }
    int nullDictId = _nullDictIds[index];
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    if (nullBitmap == null || nullBitmap.isEmpty()) {
      return dictIds;
    }
    assert _nullReplacedMultiValueDictIds != null;
    int[][] nullReplacedDictIds = _nullReplacedMultiValueDictIds[index];
    if (nullReplacedDictIds == null || nullReplacedDictIds.length < numDocs) {
      nullReplacedDictIds = new int[numDocs][];
      _nullReplacedMultiValueDictIds[index] = nullReplacedDictIds;
    }
    System.arraycopy(dictIds, 0, nullReplacedDictIds, 0, numDocs);
    PeekableIntIterator nullIterator = nullBitmap.getIntIterator();
    while (nullIterator.hasNext()) {
      // A fresh array per row rather than one shared between them: a raw key holder writes its group ids back over
      // the array it is handed for a row whose only group-by column is this one
      nullReplacedDictIds[nullIterator.next()] = new int[]{nullDictId};
    }
    return nullReplacedDictIds;
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _rawKeyHolder.getGroupIdUpperBound();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return _rawKeyHolder.getGroupKeys();
  }

  @Override
  public int getNumKeys() {
    return _rawKeyHolder.getNumKeys();
  }

  /// Clear and trim thread-local map of _rawKeyHolder
  @Override
  public void close() {
    _rawKeyHolder.close();
  }

  private interface RawKeyHolder extends AutoCloseable {

    /// Process a block of documents for all single-valued group-by columns case.
    ///
    /// @param numDocs Number of documents inside the block
    /// @param outGroupIds Buffer for group id results
    void processSingleValue(int numDocs, int[] outGroupIds);

    /// Process a block of documents for case with multi-valued group-by columns.
    ///
    /// @param numDocs Number of documents inside the block
    /// @param outGroupIds Buffer for group id results
    void processMultiValue(int numDocs, int[][] outGroupIds);

    /// Get the upper bound of group id (exclusive) inside the holder.
    ///
    /// @return Upper bound of group id inside the holder
    int getGroupIdUpperBound();

    /// Returns an iterator of [GroupKey]. Use this interface to iterate through all the group keys.
    Iterator<GroupKey> getGroupKeys();

    /// Returns current number of unique keys
    int getNumKeys();

    @Override
    void close();
  }

  // This holder works only if it can fit all results, otherwise it fails on AIOOBE or produces too many group keys
  private class ArrayBasedHolder implements RawKeyHolder {
    private final boolean[] _flags = new boolean[_globalGroupIdUpperBound];
    private int _numKeys = 0;

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      switch (_numGroupByExpressions) {
        case 1:
          processSingleValue(numDocs, _singleValueDictIds[0], outGroupIds);
          return;
        case 2:
          processSingleValue(numDocs, _singleValueDictIds[0], _singleValueDictIds[1], outGroupIds);
          return;
        case 3:
          processSingleValue(numDocs, _singleValueDictIds[0], _singleValueDictIds[1], _singleValueDictIds[2],
              outGroupIds);
          return;
        default:
      }
      processSingleValueGeneric(numDocs, outGroupIds);
    }

    private void processSingleValue(int numDocs, int[] dictIds, int[] outGroupIds) {
      System.arraycopy(dictIds, 0, outGroupIds, 0, numDocs);
      markGroups(numDocs, outGroupIds);
    }

    private void processSingleValue(int numDocs, int[] dictIds0, int[] dictIds1, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        outGroupIds[i] = dictIds1[i] * _cardinalities[0] + dictIds0[i];
      }
      markGroups(numDocs, outGroupIds);
    }

    private void processSingleValue(int numDocs, int[] dictIds0, int[] dictIds1, int[] dictIds2, int[] outGroupIds) {
      int cardinality = _cardinalities[0] * _cardinalities[1];
      for (int i = 0; i < numDocs; i++) {
        outGroupIds[i] = dictIds2[i] * cardinality + dictIds1[i] * _cardinalities[0] + dictIds0[i];
      }
      markGroups(numDocs, outGroupIds);
    }

    private void markGroups(int numDocs, int[] groupIds) {
      if (_numKeys < _globalGroupIdUpperBound) {
        for (int i = 0; i < numDocs; i++) {
          if (!_flags[groupIds[i]]) {
            _numKeys++;
            _flags[groupIds[i]] = true;
            if (_numKeys == _globalGroupIdUpperBound) {
              return;
            }
          }
        }
      }
    }

    private void processSingleValueGeneric(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int groupId = 0;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          groupId = groupId * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = groupId;
        // if the flag is false, then increase the key num
        if (!_flags[groupId]) {
          _numKeys++;
          _flags[groupId] = true;
        }
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] groupIds = getIntRawKeys(i);
        for (int groupId : groupIds) {
          if (!_flags[groupId]) {
            _numKeys++;
            _flags[groupId] = true;
          }
        }
        outGroupIds[i] = groupIds;
      }
    }

    @Override
    public int getGroupIdUpperBound() {
      return _globalGroupIdUpperBound;
    }

    @Override
    public Iterator<GroupKey> getGroupKeys() {
      return new Iterator<>() {
        private int _currentGroupId;
        private final GroupKey _groupKey = new GroupKey();

        {
          while (_currentGroupId < _globalGroupIdUpperBound && !_flags[_currentGroupId]) {
            _currentGroupId++;
          }
        }

        @Override
        public boolean hasNext() {
          return _currentGroupId < _globalGroupIdUpperBound;
        }

        @Override
        public GroupKey next() {
          _groupKey._groupId = _currentGroupId;
          _groupKey._keys = getKeys(_currentGroupId);
          _currentGroupId++;
          while (_currentGroupId < _globalGroupIdUpperBound && !_flags[_currentGroupId]) {
            _currentGroupId++;
          }
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumKeys() {
      return _numKeys;
    }

    @Override
    public void close() {
    }
  }

  private class IntMapBasedHolder implements RawKeyHolder {
    private final IntGroupIdMap _groupIdMap;

    public IntMapBasedHolder(IntGroupIdMap groupIdMap) {
      _groupIdMap = groupIdMap;
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      if (_numGroupByExpressions == 1) {
        processSingleValue(numDocs, _singleValueDictIds[0], outGroupIds);
      } else {
        processSingleValueGeneric(numDocs, outGroupIds);
      }
    }

    private void processSingleValue(int numDocs, int[] dictIds, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        outGroupIds[i] = _groupIdMap.getGroupId(dictIds[i], _globalGroupIdUpperBound);
      }
    }

    private void processSingleValueGeneric(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int rawKey = 0;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = _groupIdMap.getGroupId(rawKey, _globalGroupIdUpperBound);
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] groupIds = getIntRawKeys(i);
        int length = groupIds.length;
        for (int j = 0; j < length; j++) {
          groupIds[j] = _groupIdMap.getGroupId(groupIds[j], _globalGroupIdUpperBound);
        }
        outGroupIds[i] = groupIds;
      }
    }

    @Override
    public int getGroupIdUpperBound() {
      return _groupIdMap.size();
    }

    @Override
    public Iterator<GroupKey> getGroupKeys() {
      return new Iterator<>() {
        private final Iterator<IntGroupIdMap.Entry> _iterator = _groupIdMap.iterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          IntGroupIdMap.Entry entry = _iterator.next();
          _groupKey._groupId = entry._groupId;
          _groupKey._keys = getKeys(entry._rawKey);
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumKeys() {
      return _groupIdMap.size();
    }

    @Override
    public void close() {
      _groupIdMap.clearAndTrim();
    }
  }

  /// Helper method to calculate raw keys that can fit into integer for the given index.
  ///
  /// @param index Index in block
  /// @return Array of integer raw keys
  @SuppressWarnings("Duplicates")
  private int[] getIntRawKeys(int index) {
    int[] rawKeys = null;

    // Specialize single multi-value group-by column case
    if (_numGroupByExpressions == 1) {
      rawKeys = _multiValueDictIds[0][index];
    } else {
      // Before having to transform to array, use single value raw key for better performance
      int rawKey = 0;

      for (int i = _numGroupByExpressions - 1; i >= 0; i--) {
        int cardinality = _cardinalities[i];
        if (_isSingleValueColumn[i]) {
          int dictId = _singleValueDictIds[i][index];
          if (rawKeys == null) {
            rawKey = rawKey * cardinality + dictId;
          } else {
            int length = rawKeys.length;
            for (int j = 0; j < length; j++) {
              rawKeys[j] = rawKeys[j] * cardinality + dictId;
            }
          }
        } else {
          int[] multiValueDictIds = _multiValueDictIds[i][index];
          int numValues = multiValueDictIds.length;

          // Specialize multi-value column with only one value inside
          if (numValues == 1) {
            int dictId = multiValueDictIds[0];
            if (rawKeys == null) {
              rawKey = rawKey * cardinality + dictId;
            } else {
              int length = rawKeys.length;
              for (int j = 0; j < length; j++) {
                rawKeys[j] = rawKeys[j] * cardinality + dictId;
              }
            }
          } else {
            if (rawKeys == null) {
              rawKeys = new int[numValues];
              for (int j = 0; j < numValues; j++) {
                int dictId = multiValueDictIds[j];
                rawKeys[j] = rawKey * cardinality + dictId;
              }
            } else {
              int currentLength = rawKeys.length;
              int newLength = currentLength * numValues;
              int[] newRawKeys = new int[newLength];
              for (int j = 0; j < numValues; j++) {
                int startOffset = j * currentLength;
                System.arraycopy(rawKeys, 0, newRawKeys, startOffset, currentLength);
                int dictId = multiValueDictIds[j];
                int endOffset = startOffset + currentLength;
                for (int k = startOffset; k < endOffset; k++) {
                  newRawKeys[k] = newRawKeys[k] * cardinality + dictId;
                }
              }
              rawKeys = newRawKeys;
            }
          }
        }
      }

      if (rawKeys == null) {
        rawKeys = new int[]{rawKey};
      }
    }

    return rawKeys;
  }

  /// Helper method to get the keys from the raw key.
  private Object[] getKeys(int rawKey) {
    // Specialize single group-by column case
    if (_numGroupByExpressions == 1) {
      return new Object[]{
          _nullDictIds == null || rawKey != _nullDictIds[0] ? _dictionaries[0].getInternal(rawKey) : null
      };
    } else {
      Object[] groupKeys = new Object[_numGroupByExpressions];
      for (int i = 0; i < _numGroupByExpressions; i++) {
        int cardinality = _cardinalities[i];
        groupKeys[i] = getRawValue(i, rawKey % cardinality);
        rawKey /= cardinality;
      }
      return groupKeys;
    }
  }

  @Nullable
  private Object getRawValue(int dictionaryIndex, int dictId) {
    if (_nullDictIds != null && dictId == _nullDictIds[dictionaryIndex]) {
      return null;
    }
    Dictionary dictionary = _dictionaries[dictionaryIndex];
    Object[] table = _internedDictionaryValues[dictionaryIndex];
    if (table == null) {
      // high cardinality dictionary values aren't interned
      return dictionary.getInternal(dictId);
    }
    Object rawValue = table[dictId];
    if (rawValue == null) {
      rawValue = dictionary.getInternal(dictId);
      table[dictId] = rawValue;
    }
    return rawValue;
  }

  private class LongMapBasedHolder implements RawKeyHolder {
    private final Long2IntOpenHashMap _groupIdMap;

    public LongMapBasedHolder(Long2IntOpenHashMap groupIdMap) {
      _groupIdMap = groupIdMap;
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        long rawKey = 0L;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(rawKey);
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        long[] rawKeys = getLongRawKeys(i);
        int length = rawKeys.length;
        int[] groupIds = new int[length];
        for (int j = 0; j < length; j++) {
          groupIds[j] = getGroupId(rawKeys[j]);
        }
        outGroupIds[i] = groupIds;
      }
    }

    private int getGroupId(long rawKey) {
      int numGroups = _groupIdMap.size();
      if (numGroups < _globalGroupIdUpperBound) {
        int id = _groupIdMap.putIfAbsent(rawKey, numGroups);
        return id == INVALID_ID ? numGroups : id;
      } else {
        return _groupIdMap.get(rawKey);
      }
    }

    @Override
    public int getGroupIdUpperBound() {
      return _groupIdMap.size();
    }

    @Override
    public Iterator<GroupKey> getGroupKeys() {
      return new Iterator<>() {
        private final ObjectIterator<Long2IntMap.Entry> _iterator = _groupIdMap.long2IntEntrySet().fastIterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          Long2IntMap.Entry entry = _iterator.next();
          _groupKey._groupId = entry.getIntValue();
          _groupKey._keys = getKeys(entry.getLongKey());
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumKeys() {
      return _groupIdMap.size();
    }

    @Override
    public void close() {
      clearAndTrim(_groupIdMap);
    }
  }

  /// Helper method to calculate raw keys that can fit into integer for the given index.
  ///
  /// @param index Index in block
  /// @return Array of long raw keys
  @SuppressWarnings("Duplicates")
  private long[] getLongRawKeys(int index) {
    long[] rawKeys = null;

    // Before having to transform to array, use single value raw key for better performance
    long rawKey = 0;

    for (int i = _numGroupByExpressions - 1; i >= 0; i--) {
      int cardinality = _cardinalities[i];
      if (_isSingleValueColumn[i]) {
        int dictId = _singleValueDictIds[i][index];
        if (rawKeys == null) {
          rawKey = rawKey * cardinality + dictId;
        } else {
          int length = rawKeys.length;
          for (int j = 0; j < length; j++) {
            rawKeys[j] = rawKeys[j] * cardinality + dictId;
          }
        }
      } else {
        int[] multiValueDictIds = _multiValueDictIds[i][index];
        int numValues = multiValueDictIds.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          int dictId = multiValueDictIds[0];
          if (rawKeys == null) {
            rawKey = rawKey * cardinality + dictId;
          } else {
            int length = rawKeys.length;
            for (int j = 0; j < length; j++) {
              rawKeys[j] = rawKeys[j] * cardinality + dictId;
            }
          }
        } else {
          if (rawKeys == null) {
            rawKeys = new long[numValues];
            for (int j = 0; j < numValues; j++) {
              int dictId = multiValueDictIds[j];
              rawKeys[j] = rawKey * cardinality + dictId;
            }
          } else {
            int currentLength = rawKeys.length;
            int newLength = currentLength * numValues;
            long[] newRawKeys = new long[newLength];
            for (int j = 0; j < numValues; j++) {
              int startOffset = j * currentLength;
              System.arraycopy(rawKeys, 0, newRawKeys, startOffset, currentLength);
              int dictId = multiValueDictIds[j];
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newRawKeys[k] = newRawKeys[k] * cardinality + dictId;
              }
            }
            rawKeys = newRawKeys;
          }
        }
      }
    }

    if (rawKeys == null) {
      return new long[]{rawKey};
    } else {
      return rawKeys;
    }
  }

  /// Helper method to get the keys from the raw key.
  private Object[] getKeys(long rawKey) {
    Object[] groupKeys = new Object[_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      int cardinality = _cardinalities[i];
      groupKeys[i] = getRawValue(i, (int) (rawKey % cardinality));
      rawKey /= cardinality;
    }
    return groupKeys;
  }

  private class ArrayMapBasedHolder implements RawKeyHolder {
    private final Object2IntOpenHashMap<IntArray> _groupIdMap;

    public ArrayMapBasedHolder(Object2IntOpenHashMap<IntArray> groupIdMap) {
      _groupIdMap = groupIdMap;
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] dictIds = new int[_numGroupByExpressions];
        for (int j = 0; j < _numGroupByExpressions; j++) {
          dictIds[j] = _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(new IntArray(dictIds));
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        IntArray[] rawKeys = getIntArrayRawKeys(i);
        int length = rawKeys.length;
        int[] groupIds = new int[length];
        for (int j = 0; j < length; j++) {
          groupIds[j] = getGroupId(rawKeys[j]);
        }
        outGroupIds[i] = groupIds;
      }
    }

    private int getGroupId(IntArray rawKey) {
      int numGroups = _groupIdMap.size();
      if (numGroups < _globalGroupIdUpperBound) {
        return _groupIdMap.computeIfAbsent(rawKey, (ToIntFunction<? super IntArray>) k -> numGroups);
      } else {
        return _groupIdMap.getInt(rawKey);
      }
    }

    @Override
    public int getGroupIdUpperBound() {
      return _groupIdMap.size();
    }

    @Override
    public Iterator<GroupKey> getGroupKeys() {
      return new Iterator<>() {
        private final ObjectIterator<Object2IntMap.Entry<IntArray>> _iterator =
            _groupIdMap.object2IntEntrySet().fastIterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          Object2IntMap.Entry<IntArray> entry = _iterator.next();
          _groupKey._groupId = entry.getIntValue();
          _groupKey._keys = getKeys(entry.getKey());
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumKeys() {
      return _groupIdMap.size();
    }

    @Override
    public void close() {
      clearAndTrim(_groupIdMap);
    }
  }

  /// Helper method to calculate raw keys that can fit into integer for the given index.
  ///
  /// @param index Index in block
  /// @return Array of IntArray raw keys
  @SuppressWarnings("Duplicates")
  private IntArray[] getIntArrayRawKeys(int index) {
    IntArray[] rawKeys = null;

    // Before having to transform to array, use single value raw key for better performance
    int[] dictIds = new int[_numGroupByExpressions];

    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (_isSingleValueColumn[i]) {
        int dictId = _singleValueDictIds[i][index];
        if (rawKeys == null) {
          dictIds[i] = dictId;
        } else {
          for (IntArray rawKey : rawKeys) {
            rawKey._elements[i] = dictId;
          }
        }
      } else {
        int[] multiValueDictIds = _multiValueDictIds[i][index];
        int numValues = multiValueDictIds.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          int dictId = multiValueDictIds[0];
          if (rawKeys == null) {
            dictIds[i] = dictId;
          } else {
            for (IntArray rawKey : rawKeys) {
              rawKey._elements[i] = dictId;
            }
          }
        } else {
          if (rawKeys == null) {
            rawKeys = new IntArray[numValues];
            for (int j = 0; j < numValues; j++) {
              int dictId = multiValueDictIds[j];
              rawKeys[j] = new IntArray(dictIds.clone());
              rawKeys[j]._elements[i] = dictId;
            }
          } else {
            int currentLength = rawKeys.length;
            int newLength = currentLength * numValues;
            IntArray[] newRawKeys = new IntArray[newLength];
            System.arraycopy(rawKeys, 0, newRawKeys, 0, currentLength);
            for (int j = 1; j < numValues; j++) {
              int offset = j * currentLength;
              for (int k = 0; k < currentLength; k++) {
                newRawKeys[offset + k] = new IntArray(rawKeys[k]._elements.clone());
              }
            }
            for (int j = 0; j < numValues; j++) {
              int startOffset = j * currentLength;
              int dictId = multiValueDictIds[j];
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newRawKeys[k]._elements[i] = dictId;
              }
            }
            rawKeys = newRawKeys;
          }
        }
      }
    }

    if (rawKeys == null) {
      return new IntArray[]{new IntArray(dictIds)};
    } else {
      return rawKeys;
    }
  }

  /// Helper method to get the keys from the raw key.
  private Object[] getKeys(IntArray rawKey) {
    Object[] groupKeys = new Object[_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      groupKeys[i] = getRawValue(i, rawKey._elements[i]);
    }
    return groupKeys;
  }

  /// Fast int-to-int hashmap with [#INVALID_ID] as the default return value.
  ///
  /// Different from [it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap], this map uses one single array to store
  /// keys and values to reduce the cache miss.
  @VisibleForTesting
  public static class IntGroupIdMap {
    private static final float LOAD_FACTOR = 0.75f;

    private int[] _keyValueHolder;
    private int _capacity;
    private int _mask;
    private int _maxNumEntries;
    private int _size;

    public IntGroupIdMap() {
      init();
    }

    private void init() {
      // Initialize the map with capacity 512 so that the _keyValueHolder can fit into a single memory page
      _capacity = 1 << 9;
      int holderSize = _capacity << 1;
      _keyValueHolder = new int[holderSize];
      _mask = holderSize - 1;
      _maxNumEntries = (int) (_capacity * LOAD_FACTOR);
    }

    public int size() {
      return _size;
    }

    /// Returns the group id for the given raw key. Create a new group id if the raw key does not exist and the group id
    /// upper bound is not reached.
    public int getGroupId(int rawKey, int groupIdUpperBound) {
      // NOTE: Key 0 is reserved as the null key. Use (rawKey + 1) as the internal key because rawKey can never be -1.
      int internalKey = rawKey + 1;
      int index = (HashCommon.mix(internalKey) << 1) & _mask;
      int key = _keyValueHolder[index];

      // Handle hash hit separately for better performance
      if (key == internalKey) {
        return _keyValueHolder[index + 1];
      }
      if (key == 0) {
        return _size < groupIdUpperBound ? addNewGroup(internalKey, index) : INVALID_ID;
      }

      // Hash collision
      while (true) {
        index = (index + 2) & _mask;
        key = _keyValueHolder[index];
        if (key == internalKey) {
          return _keyValueHolder[index + 1];
        }
        if (key == 0) {
          return _size < groupIdUpperBound ? addNewGroup(internalKey, index) : INVALID_ID;
        }
      }
    }

    private int addNewGroup(int internalKey, int index) {
      int groupId = _size++;
      _keyValueHolder[index] = internalKey;
      _keyValueHolder[index + 1] = groupId;
      if (_size > _maxNumEntries) {
        expand();
      }
      return groupId;
    }

    private void expand() {
      _capacity <<= 1;
      int holderSize = _capacity << 1;
      int[] oldKeyValueHolder = _keyValueHolder;
      _keyValueHolder = new int[holderSize];
      _mask = holderSize - 1;
      _maxNumEntries <<= 1;
      int oldIndex = 0;
      for (int i = 0; i < _size; i++) {
        while (oldKeyValueHolder[oldIndex] == 0) {
          oldIndex += 2;
        }
        int key = oldKeyValueHolder[oldIndex];
        int value = oldKeyValueHolder[oldIndex + 1];
        int newIndex = (HashCommon.mix(key) << 1) & _mask;
        if (_keyValueHolder[newIndex] != 0) {
          do {
            newIndex = (newIndex + 2) & _mask;
          } while (_keyValueHolder[newIndex] != 0);
        }
        _keyValueHolder[newIndex] = key;
        _keyValueHolder[newIndex + 1] = value;
        oldIndex += 2;
      }
    }

    public Iterator<Entry> iterator() {
      return new Iterator<>() {
        private final Entry _entry = new Entry();
        private int _index;
        private int _numRemainingEntries = _size;

        @Override
        public boolean hasNext() {
          return _numRemainingEntries > 0;
        }

        @Override
        public Entry next() {
          int key;
          while ((key = _keyValueHolder[_index]) == 0) {
            _index += 2;
          }
          _entry._rawKey = key - 1;
          _entry._groupId = _keyValueHolder[_index + 1];
          _index += 2;
          _numRemainingEntries--;
          return _entry;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    /// Clears the map and trims the map if the size is larger than the [#MAX_CACHING_MAP_SIZE].
    public void clearAndTrim() {
      if (_size == 0) {
        return;
      }
      if (_size <= MAX_CACHING_MAP_SIZE) {
        // Clear the map
        Arrays.fill(_keyValueHolder, 0);
      } else {
        // Init the map (clear and trim)
        init();
      }
      _size = 0;
    }

    public static class Entry {
      public int _rawKey;
      public int _groupId;
    }
  }

  /// Drop un-necessary checks for highest performance.
  @VisibleForTesting
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  static class IntArray {
    public int[] _elements;

    public IntArray(int[] elements) {
      _elements = elements;
    }

    @Override
    public int hashCode() {
      int result = 1;
      for (int element : _elements) {
        result = 31 * result + element;
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      int[] that = ((IntArray) obj)._elements;
      int length = _elements.length;
      if (length != that.length) {
        return false;
      }
      for (int i = 0; i < length; i++) {
        if (_elements[i] != that[i]) {
          return false;
        }
      }
      return true;
    }
  }
}
