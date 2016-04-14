/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.groupby;

import com.google.common.primitives.Longs;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class MultiValueGroupKeyGenerator implements GroupKeyGenerator {
  private final BlockSingleValIterator[] _singleValIterators;
  private final BlockMultiValIterator[] _multiValIterators;

  private final boolean[] _isSingleValueGroupByColumn;
  private final String[] _groupByColumns;
  private final int[] _cardinalities;

  // Re-usable array to read multi-values per columns.
  private final int[][] _maxMultiValuesArray;
  private int[] _reusableGroupByValuesArray;
  private Dictionary[] _dictionaries;

  private LongSet _uniqueGroupKeys;

  public MultiValueGroupKeyGenerator(IndexSegment indexSegment, GroupBy groupBy) {
    List<String> groupByColumns = groupBy.getColumns();
    _uniqueGroupKeys = new LongOpenHashSet();

    _groupByColumns = groupByColumns.toArray(new String[groupByColumns.size()]);
    int numGroupByColumns = _groupByColumns.length;

    _maxMultiValuesArray = new int[numGroupByColumns][];
    _cardinalities = new int[numGroupByColumns];
    _dictionaries = new Dictionary[numGroupByColumns];

    _reusableGroupByValuesArray = new int[numGroupByColumns];
    _isSingleValueGroupByColumn = new boolean[numGroupByColumns];

    _singleValIterators = new BlockSingleValIterator[numGroupByColumns];
    _multiValIterators = new BlockMultiValIterator[numGroupByColumns];

    for (int i = 0; i < _groupByColumns.length; i++) {
      String groupByColumn = _groupByColumns[i];
      DataSource dataSource = indexSegment.getDataSource(groupByColumn);
      _dictionaries[i] = dataSource.getDictionary();

      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      _cardinalities[i] = dataSourceMetadata.cardinality();

      Block block = dataSource.nextBlock();
      int maxNumberOfMultiValues = block.getMetadata().getMaxNumberOfMultiValues();
      _maxMultiValuesArray[i] = new int[maxNumberOfMultiValues];

      if (dataSourceMetadata.isSingleValue()) {
        _isSingleValueGroupByColumn[i] = true;
        _singleValIterators[i] = (BlockSingleValIterator) block.getBlockValueSet().iterator();
      } else {
        _isSingleValueGroupByColumn[i] = false;
        _multiValIterators[i] = (BlockMultiValIterator) block.getBlockValueSet().iterator();
      }
    }
  }

  /**
   * Given a docId, generate and return an array of unique group by keys, based on the
   * values of group by columns of the record of given docId.
   *
   * @param docId
   * @return
   */
  public long[] generateKeysForDocId(int docId) {
    List<Long> groupByKeys = new ArrayList<>();
    groupByKeys.add(0l);

    for (int i = 0; i < _groupByColumns.length; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        BlockSingleValIterator blockValIterator = _singleValIterators[i];
        blockValIterator.skipTo(docId);
        int dictId = blockValIterator.nextIntVal();

        for (int j = 0; j < groupByKeys.size(); j++) {
          groupByKeys.set(j, groupByKeys.get(j) * _cardinalities[i] + dictId);
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_maxMultiValuesArray[i]);

        int originalSize = groupByKeys.size();
        for (int j = 0; j < numMultiValues - 1; ++j) {
          for (int k = 0; k < originalSize; k++) {
            groupByKeys.add(groupByKeys.get(k));
          }
        }

        for (int j = 0; j < numMultiValues; j++) {
          for (int k = 0; k < originalSize; k++) {
            int index = j * originalSize + k;
            groupByKeys.set(index, (groupByKeys.get(index) * _cardinalities[i]) + _maxMultiValuesArray[i][j]);
          }
        }
      }
    }

    _uniqueGroupKeys.addAll(groupByKeys);
    return Longs.toArray(groupByKeys);
  }

  /**
   * Given a docIdSet, return a map containing array of group by keys for each docId.
   * The key in the map returned is indexed from startIndex to startIndex + length.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   * @return
   */
  public Int2ObjectOpenHashMap generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length) {
    Int2ObjectOpenHashMap docIdToGroupKeysMap = new Int2ObjectOpenHashMap(length);

    int endIndex = startIndex + length - 1;
    for (int i = startIndex; i <= endIndex; ++i) {
      int docId = docIdSet[i];

      // Note this is key in the map is the index not docId.
      docIdToGroupKeysMap.put(i, generateKeysForDocId(docId));
    }
    return docIdToGroupKeysMap;
  }

  /**
   * Given a group by key, decode and return the corresponding dict id's for the group
   * by columns, in the passed in array.
   *
   * @param groupKey
   * @param decoded
   */
  public void decodeGroupKey(long groupKey, int[] decoded) {
    decodeGroupKey(groupKey, _cardinalities, decoded);
  }

  /**
   * Decode the individual column values from a given group by key and the individual
   * column cardinalities.
   *
   * @param groupKey
   * @param cardinalities
   * @param decoded
   */
  private static void decodeGroupKey(long groupKey, int[] cardinalities, int[] decoded) {
    int length = cardinalities.length;

    for (int i = length - 1; i >= 0; --i) {
      decoded[i] = (int) (groupKey % cardinalities[i]);
      groupKey = groupKey / cardinalities[i];
    }
  }

  /**
   * {@inheritDoc}
   * @param docId
   * @return
   */
  @Override
  public int generateKeyForDocId(int docId) {
    throw new RuntimeException("Unsupported method, as MultiValueGroupKeyGenerator returns multiple keys per docId");
  }

  /**
   * {@inheritDoc}
   * @param docIdSet
   * @param startIndex
   * @param length
   * @param docIdToGroupKey
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey) {
    throw new RuntimeException("Unsupported method, MultiValueGroupKeyGenerator returns multiple keys per docId");
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getNumGroupKeys() {
    return _uniqueGroupKeys.size();
  }


  public Iterator<Pair<Long, String>> getUniqueGroupKeys() {
    LongIterator iterator = _uniqueGroupKeys.iterator();
    return new GroupKeyIterator(iterator);
  }

  /**
   * Convert group key from dictId based to string based, using actually values
   * corresponding to dictionary id's.
   *
   * @param groupKey
   * @return
   */
  private String dictIdToStringGroupKey(long groupKey) {
    decodeGroupKey(groupKey, _cardinalities, _reusableGroupByValuesArray);

    // Special case one group by column for performance.
    if (_groupByColumns.length == 1) {
      return _dictionaries[0].get(_reusableGroupByValuesArray[0]).toString();
    } else {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < _reusableGroupByValuesArray.length; i++) {
        String key = _dictionaries[i].get(_reusableGroupByValuesArray[i]).toString();

        if (i > 0) {
          builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString());
        }
        builder.append(key);
      }

      return builder.toString();
    }
  }

  /**
   * Inner class to implement group by keys iterator for MAP_BASED storage.
   */
  private class GroupKeyIterator implements Iterator<Pair<Long, String>> {
    private final LongIterator _iterator;
    Pair<Long, String> ret;

    public GroupKeyIterator(LongIterator iterator) {
      _iterator = iterator;
      ret = new Pair<Long, String>(-1L, null);
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public Pair<Long, String> next() {
      long groupKey = _iterator.next();

      String stringGroupKey = dictIdToStringGroupKey(groupKey);
      ret.setFirst(groupKey);
      ret.setSecond(stringGroupKey);
      return ret;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported remove method.");
    }
  }
}
