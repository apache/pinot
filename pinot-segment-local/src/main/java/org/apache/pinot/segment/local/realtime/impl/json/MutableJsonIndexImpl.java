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
package org.apache.pinot.segment.local.realtime.impl.json;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Json index for mutable segment.
 */
public class MutableJsonIndexImpl implements MutableJsonIndex {
  private final JsonIndexConfig _jsonIndexConfig;
  private final TreeMap<String, RoaringBitmap> _postingListMap;
  private final IntList _docIdMapping;
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  private int _nextDocId;
  private int _nextFlattenedDocId;

  public MutableJsonIndexImpl(JsonIndexConfig jsonIndexConfig) {
    _jsonIndexConfig = jsonIndexConfig;
    _postingListMap = new TreeMap<>();
    _docIdMapping = new IntArrayList();

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  /**
   * Adds the next json value.
   */
  @Override
  public void add(String jsonString)
      throws IOException {
    try {
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonString, _jsonIndexConfig);
      _writeLock.lock();
      try {
        addFlattenedRecords(flattenedRecords);
      } finally {
        _writeLock.unlock();
      }
    } finally {
      _nextDocId++;
    }
  }

  /**
   * Adds the flattened records for the next document.
   */
  private void addFlattenedRecords(List<Map<String, String>> records) {
    int numRecords = records.size();
    Preconditions.checkState(_nextFlattenedDocId + numRecords >= 0, "Got more than %s flattened records",
        Integer.MAX_VALUE);
    for (int i = 0; i < numRecords; i++) {
      _docIdMapping.add(_nextDocId);
    }
    // TODO: Consider storing tuples as the key of the posting list so that the strings can be reused, and the hashcode
    //       can be cached.
    for (Map<String, String> record : records) {
      for (Map.Entry<String, String> entry : record.entrySet()) {
        // Put both key and key-value into the posting list. Key is useful for checking if a key exists in the json.
        String key = entry.getKey();
        _postingListMap.computeIfAbsent(key, k -> new RoaringBitmap()).add(_nextFlattenedDocId);
        String keyValue = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + entry.getValue();
        _postingListMap.computeIfAbsent(keyValue, k -> new RoaringBitmap()).add(_nextFlattenedDocId);
      }
      _nextFlattenedDocId++;
    }
  }

  @Override
  public MutableRoaringBitmap getMatchingDocIds(String filterString) {
    FilterContext filter;
    try {
      filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
      Preconditions.checkArgument(!filter.isConstant());
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid json match filter: " + filterString);
    }

    _readLock.lock();
    try {
      if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
        // Handle exclusive predicate separately because the flip can only be applied to the unflattened doc ids in
        // order to get the correct result, and it cannot be nested
        RoaringBitmap matchingFlattenedDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        matchingFlattenedDocIds.forEach(
            (IntConsumer) flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
        matchingDocIds.flip(0, (long) _nextDocId);
        return matchingDocIds;
      } else {
        RoaringBitmap matchingFlattenedDocIds = getMatchingFlattenedDocIds(filter);
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        matchingFlattenedDocIds.forEach(
            (IntConsumer) flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
        return matchingDocIds;
      }
    } finally {
      _readLock.unlock();
    }
  }

  /**
   * Returns {@code true} if the given predicate type is exclusive for json_match calculation, {@code false} otherwise.
   */
  private boolean isExclusive(Predicate.Type predicateType) {
    return predicateType == Predicate.Type.NOT_EQ || predicateType == Predicate.Type.NOT_IN
        || predicateType == Predicate.Type.IS_NULL;
  }

  /**
   * Returns the matching flattened doc ids for the given filter.
   */
  private RoaringBitmap getMatchingFlattenedDocIds(FilterContext filter) {
    switch (filter.getType()) {
      case AND: {
        List<FilterContext> children = filter.getChildren();
        int numChildren = children.size();
        RoaringBitmap matchingDocIds = getMatchingFlattenedDocIds(children.get(0));
        for (int i = 1; i < numChildren; i++) {
          matchingDocIds.and(getMatchingFlattenedDocIds(children.get(i)));
        }
        return matchingDocIds;
      }
      case OR: {
        List<FilterContext> children = filter.getChildren();
        int numChildren = children.size();
        RoaringBitmap matchingDocIds = getMatchingFlattenedDocIds(children.get(0));
        for (int i = 1; i < numChildren; i++) {
          matchingDocIds.or(getMatchingFlattenedDocIds(children.get(i)));
        }
        return matchingDocIds;
      }
      case PREDICATE: {
        Predicate predicate = filter.getPredicate();
        Preconditions.checkArgument(!isExclusive(predicate.getType()), "Exclusive predicate: %s cannot be nested",
            predicate);
        return getMatchingFlattenedDocIds(predicate);
      }
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Returns the matching flattened doc ids for the given predicate.
   * <p>Exclusive predicate is handled as the inclusive predicate, and the caller should flip the unflattened doc ids in
   * order to get the correct exclusive predicate result.
   */
  private RoaringBitmap getMatchingFlattenedDocIds(Predicate predicate) {
    ExpressionContext lhs = predicate.getLhs();
    Preconditions.checkArgument(lhs.getType() == ExpressionContext.Type.IDENTIFIER,
        "Left-hand side of the predicate must be an identifier, got: %s (%s). Put double quotes around the identifier"
            + " if needed.", lhs, lhs.getType());
    String key = lhs.getIdentifier();

    // Support 2 formats:
    // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
    // - Legacy format (e.g. "a[1].b"='abc')
    if (key.charAt(0) == '$') {
      key = key.substring(1);
    } else {
      key = JsonUtils.KEY_SEPARATOR + key;
    }
    Pair<String, RoaringBitmap> pair = getKeyAndFlattenDocId(key);
    key = pair.getLeft();
    RoaringBitmap matchingDocIds = pair.getRight();
    if (matchingDocIds != null && matchingDocIds.isEmpty()) {
      return new RoaringBitmap();
    }

    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ || predicateType == Predicate.Type.NOT_EQ) {
      String value = predicateType == Predicate.Type.EQ ? ((EqPredicate) predicate).getValue()
          : ((NotEqPredicate) predicate).getValue();
      String keyValuePair = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
      RoaringBitmap matchingDocIdsForKeyValuePair = _postingListMap.get(keyValuePair);
      if (matchingDocIdsForKeyValuePair != null) {
        if (matchingDocIds == null) {
          return matchingDocIdsForKeyValuePair.clone();
        } else {
          matchingDocIds.and(matchingDocIdsForKeyValuePair);
          return matchingDocIds;
        }
      } else {
        return new RoaringBitmap();
      }
    } else if (predicateType == Predicate.Type.IN || predicateType == Predicate.Type.NOT_IN) {
      List<String> values = predicateType == Predicate.Type.IN ? ((InPredicate) predicate).getValues()
          : ((NotInPredicate) predicate).getValues();
      RoaringBitmap matchingDocIdsForKeyValuePairs = new RoaringBitmap();
      for (String value : values) {
        String keyValuePair = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
        RoaringBitmap matchingDocIdsForKeyValuePair = _postingListMap.get(keyValuePair);
        if (matchingDocIdsForKeyValuePair != null) {
          matchingDocIdsForKeyValuePairs.or(matchingDocIdsForKeyValuePair);
        }
      }
      if (matchingDocIds == null) {
        return matchingDocIdsForKeyValuePairs;
      } else {
        matchingDocIds.and(matchingDocIdsForKeyValuePairs);
        return matchingDocIds;
      }
    } else if (predicateType == Predicate.Type.IS_NOT_NULL || predicateType == Predicate.Type.IS_NULL) {
      RoaringBitmap matchingDocIdsForKey = _postingListMap.get(key);
      if (matchingDocIdsForKey != null) {
        if (matchingDocIds == null) {
          return matchingDocIdsForKey.clone();
        } else {
          matchingDocIds.and(matchingDocIdsForKey);
          return matchingDocIds;
        }
      } else {
        return new RoaringBitmap();
      }
    } else if (predicateType == Predicate.Type.REGEXP_LIKE) {
      Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
      if (subMap.isEmpty()) {
        return new RoaringBitmap();
      }
      Pattern pattern = ((RegexpLikePredicate) predicate).getPattern();
      RoaringBitmap result = null;

      for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
        if (!pattern.matcher(entry.getKey().substring(key.length() + 1)).matches()) {
          continue;
        }
        if (result == null) {
          result = entry.getValue().clone();
        } else {
          result.or(entry.getValue());
        }
      }

      if (result == null) {
        return new RoaringBitmap();
      } else {
        if (matchingDocIds == null) {
          return result;
        } else {
          matchingDocIds.and(result);
          return matchingDocIds;
        }
      }
    } else if (predicateType == Predicate.Type.RANGE) {
      Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
      if (subMap.isEmpty()) {
        return new RoaringBitmap();
      }
      RoaringBitmap result = null;

      RangePredicate rangePredicate = (RangePredicate) predicate;
      FieldSpec.DataType rangeDataType = rangePredicate.getRangeDataType();
      // Simplify to only support numeric and string types
      if (rangeDataType.isNumeric()) {
        rangeDataType = FieldSpec.DataType.DOUBLE;
      } else {
        rangeDataType = FieldSpec.DataType.STRING;
      }

      boolean lowerUnbounded = rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED);
      boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
      Object lowerBound = lowerUnbounded ? null : rangeDataType.convert(rangePredicate.getLowerBound());
      Object upperBound = upperUnbounded ? null : rangeDataType.convert(rangePredicate.getUpperBound());

      for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
        Object valueObj = rangeDataType.convert(entry.getKey().substring(key.length() + 1));
        boolean lowerCompareResult =
            lowerUnbounded || (lowerInclusive ? rangeDataType.compare(valueObj, lowerBound) >= 0
                : rangeDataType.compare(valueObj, lowerBound) > 0);
        boolean upperCompareResult =
            upperUnbounded || (upperInclusive ? rangeDataType.compare(valueObj, upperBound) <= 0
                : rangeDataType.compare(valueObj, upperBound) < 0);
        if (lowerCompareResult && upperCompareResult) {
          if (result == null) {
            result = entry.getValue().clone();
          } else {
            result.or(entry.getValue());
          }
        }
      }

      if (result == null) {
        return new RoaringBitmap();
      } else {
        if (matchingDocIds == null) {
          return result;
        } else {
          matchingDocIds.and(result);
          return matchingDocIds;
        }
      }
    } else {
      throw new IllegalStateException("Unsupported json_match predicate type: " + predicate);
    }
  }

  @VisibleForTesting
  public Map<String, RoaringBitmap> convertFlattenedDocIdsToDocIds(Map<String, RoaringBitmap> valueToFlattenedDocIds) {
    _readLock.lock();
    try {
      Map<String, RoaringBitmap> valueToDocIds = new HashMap<>();
      for (Map.Entry<String, RoaringBitmap> entry : valueToFlattenedDocIds.entrySet()) {
        RoaringBitmap docIds = new RoaringBitmap();
        entry.getValue().forEach((IntConsumer) flattenedDocId -> docIds.add(_docIdMapping.getInt(flattenedDocId)));
        valueToDocIds.put(entry.getKey(), docIds);
      }
      return valueToDocIds;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public Map<String, RoaringBitmap> getMatchingFlattenedDocsMap(String jsonPathKey) {
    Map<String, RoaringBitmap> valueToMatchingFlattenedDocIdsMap = new HashMap<>();
    _readLock.lock();
    try {
      Pair<String, RoaringBitmap> result = getKeyAndFlattenDocId(jsonPathKey);
      jsonPathKey = result.getLeft();
      RoaringBitmap arrayIndexFlattenDocIds = result.getRight();
      if (arrayIndexFlattenDocIds != null && arrayIndexFlattenDocIds.isEmpty()) {
        return valueToMatchingFlattenedDocIdsMap;
      }
      Map<String, RoaringBitmap> subMap = getMatchingKeysMap(jsonPathKey);
      for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
        RoaringBitmap flattenedDocIds = entry.getValue().clone();
        if (arrayIndexFlattenDocIds != null) {
          flattenedDocIds.and(arrayIndexFlattenDocIds);
        }
        if (!flattenedDocIds.isEmpty()) {
          valueToMatchingFlattenedDocIdsMap.put(entry.getKey().substring(jsonPathKey.length() + 1), flattenedDocIds);
        }
      }

      return valueToMatchingFlattenedDocIdsMap;
    } finally {
      _readLock.unlock();
    }
  }

  /**
   *  If key doesn't contain the array index, return <original key, null bitmap>
   *  Elif the key, i.e. the json path provided by user doesn't match any data, return <null, empty bitmap>
   *  Else, return the json path that is generated by replacing array index with . on the original key
   *  and the associated flattenDocId bitmap
   */
  private Pair<String, RoaringBitmap> getKeyAndFlattenDocId(String key) {
    // Process the array index within the key if exists
    // E.g. "[*]"=1 -> "."='1'
    // E.g. "[0]"=1 -> ".$index"='0' && "."='1'
    // E.g. "[0][1]"=1 -> ".$index"='0' && "..$index"='1' && ".."='1'
    // E.g. ".foo[*].bar[*].foobar"='abc' -> ".foo..bar..foobar"='abc'
    // E.g. ".foo[0].bar[1].foobar"='abc' -> ".foo.$index"='0' && ".foo..bar.$index"='1' && ".foo..bar..foobar"='abc'
    // E.g. ".foo[0][1].bar"='abc' -> ".foo.$index"='0' && ".foo..$index"='1' && ".foo...bar"='abc'
    RoaringBitmap matchingDocIds = null;
    int leftBracketIndex;
    while ((leftBracketIndex = key.indexOf('[')) >= 0) {
      int rightBracketIndex = key.indexOf(']', leftBracketIndex + 2);
      Preconditions.checkArgument(rightBracketIndex > 0, "Missing right bracket in key: %s", key);

      String leftPart = key.substring(0, leftBracketIndex);
      String arrayIndex = key.substring(leftBracketIndex + 1, rightBracketIndex);
      String rightPart = key.substring(rightBracketIndex + 1);

      if (!arrayIndex.equals(JsonUtils.WILDCARD)) {
        // "[0]"=1 -> ".$index"='0' && "."='1'
        // ".foo[1].bar"='abc' -> ".foo.$index"=1 && ".foo..bar"='abc'
        String searchKey = leftPart + JsonUtils.ARRAY_INDEX_KEY + JsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
        RoaringBitmap docIds = _postingListMap.get(searchKey);
        if (docIds != null) {
          if (matchingDocIds == null) {
            matchingDocIds = docIds.clone();
          } else {
            matchingDocIds.and(docIds);
          }
        } else {
          return Pair.of(null, new RoaringBitmap());
        }
      }

      key = leftPart + JsonUtils.KEY_SEPARATOR + rightPart;
    }
    return Pair.of(key, matchingDocIds);
  }

  private Map<String, RoaringBitmap> getMatchingKeysMap(String key) {
    return _postingListMap.subMap(key + JsonIndexCreator.KEY_VALUE_SEPARATOR, false,
        key + JsonIndexCreator.KEY_VALUE_SEPARATOR_NEXT_CHAR, false);
  }

  @Override
  public String[][] getValuesForMv(int[] docIds, int length,
      Map<String, RoaringBitmap> valueToMatchingFlattenedDocs) {
    String[][] result = new String[length][];
    List<PriorityQueue<Pair<String, Integer>>> docIdToFlattenedDocIdsAndValues = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      // Sort based on flattened doc id
      docIdToFlattenedDocIdsAndValues.add(new PriorityQueue<>(Comparator.comparingInt(Pair::getRight)));
    }
    Map<Integer, Integer> docIdToPos = new HashMap<>();
    for (int i = 0; i < length; i++) {
      docIdToPos.put(docIds[i], i);
    }

    _readLock.lock();
    try {
      for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
        String value = entry.getKey();
        RoaringBitmap matchingFlattenedDocIds = entry.getValue();
        matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> {
          int docId = _docIdMapping.getInt(flattenedDocId);
          if (docIdToPos.containsKey(docId)) {
            docIdToFlattenedDocIdsAndValues.get(docIdToPos.get(docId)).add(Pair.of(value, flattenedDocId));
          }
        });
      }
    } finally {
      _readLock.unlock();
    }

    for (int i = 0; i < length; i++) {
      PriorityQueue<Pair<String, Integer>> pq = docIdToFlattenedDocIdsAndValues.get(i);
      result[i] = new String[pq.size()];
      int j = 0;
      while (!pq.isEmpty()) {
        result[i][j++] = pq.poll().getLeft();
      }
    }

    return result;
  }

  @Override
  public String[] getValuesForSv(int[] docIds, int length, Map<String, RoaringBitmap> valueToMatchingFlattenedDocs) {
    Int2ObjectOpenHashMap<String> docIdToValues = new Int2ObjectOpenHashMap<>(length);
    RoaringBitmap docIdMask = RoaringBitmap.bitmapOf(Arrays.copyOfRange(docIds, 0, length));
    _readLock.lock();
    try {
      for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
        String value = entry.getKey();
        RoaringBitmap matchingFlattenedDocIds = entry.getValue();
        matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> {
          int docId = _docIdMapping.getInt(flattenedDocId);
          if (docIdMask.contains(docId)) {
            docIdToValues.put(docId, value);
          }
        });
      }
    } finally {
      _readLock.unlock();
    }

    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = docIdToValues.get(docIds[i]);
    }
    return values;
  }

  @Override
  public void close() {
  }
}
