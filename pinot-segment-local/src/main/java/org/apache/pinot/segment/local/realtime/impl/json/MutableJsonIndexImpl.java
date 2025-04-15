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
import javax.annotation.Nullable;
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
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.Tracing;
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
        LazyBitmap flattenedDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        flattenedDocIds.forEach(flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
        matchingDocIds.flip(0, (long) _nextDocId);
        return matchingDocIds;
      } else {
        LazyBitmap flattenedDocIds = getMatchingFlattenedDocIds(filter);
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        flattenedDocIds.forEach(flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
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
    return predicateType == Predicate.Type.IS_NULL;
  }

  /** This class allows delaying of cloning posting list bitmap for as long as possible
   * It stores either a bitmap from posting list that must be cloned before mutating (readOnly=true)
   * or an already  cloned bitmap.
   */
  static class LazyBitmap {

    final static LazyBitmap EMPTY_BITMAP = new LazyBitmap(null);

    // value should be null only for EMPTY
    @Nullable
    RoaringBitmap _value;

    // if readOnly then bitmap needs to be cloned before applying mutating operations
    boolean _readOnly;

    LazyBitmap(RoaringBitmap bitmap) {
      _value = bitmap;
      _readOnly = true;
    }

    LazyBitmap(RoaringBitmap bitmap, boolean isReadOnly) {
      _value = bitmap;
      _readOnly = isReadOnly;
    }

    boolean isMutable() {
      return !_readOnly;
    }

    LazyBitmap toMutable() {
      if (_readOnly) {
        if (_value == null) {
          return new LazyBitmap(new RoaringBitmap(), false);
        }

        _value = _value.clone();
        _readOnly = false;
      }

      return this;
    }

    void and(LazyBitmap bitmap) {
      assert isMutable();

      _value.and(bitmap._value);
    }

    LazyBitmap and(RoaringBitmap bitmap) {
      LazyBitmap mutable = toMutable();
      mutable._value.and(bitmap);
      return mutable;
    }

    LazyBitmap andNot(RoaringBitmap bitmap) {
      LazyBitmap mutable = toMutable();
      mutable._value.andNot(bitmap);
      return mutable;
    }

    void or(LazyBitmap bitmap) {
      assert isMutable();

      _value.or(bitmap._value);
    }

    LazyBitmap or(RoaringBitmap bitmap) {
      LazyBitmap mutable = toMutable();
      mutable._value.or(bitmap);
      return mutable;
    }

    boolean isEmpty() {
      if (_value == null) {
        return true;
      } else {
        return _value.isEmpty();
      }
    }

    void forEach(IntConsumer ic) {
      if (_value != null) {
        _value.forEach(ic);
      }
    }

    LazyBitmap flip(long rangeStart, long rangeEnd) {
      LazyBitmap result = toMutable();
      result._value.flip(rangeStart, rangeEnd);
      return result;
    }

    RoaringBitmap getValue() {
      if (_value == null) {
        return new RoaringBitmap();
      } else {
        return _value;
      }
    }
  }

  /**
   * Returns the matching flattened doc ids for the given filter.
   */
  private LazyBitmap getMatchingFlattenedDocIds(FilterContext filter) {
    switch (filter.getType()) {
      case AND: {
        List<FilterContext> filters = filter.getChildren();
        LazyBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));
        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          if (matchingDocIds.isEmpty()) {
            break;
          }

          LazyBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          if (filterDocIds.isEmpty()) {
            return filterDocIds;
          } else {
            matchingDocIds = and(matchingDocIds, filterDocIds);
          }
        }
        return matchingDocIds;
      }
      case OR: {
        List<FilterContext> filters = filter.getChildren();
        LazyBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));

        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          LazyBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          // avoid having to convert matchingDocIds to mutable map
          if (filterDocIds.isEmpty()) {
            continue;
          }

          matchingDocIds = or(matchingDocIds, filterDocIds);
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
  private LazyBitmap getMatchingFlattenedDocIds(Predicate predicate) {
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
    Pair<String, LazyBitmap> pair = getKeyAndFlattenedDocIds(key);
    key = pair.getLeft();
    LazyBitmap matchingDocIds = pair.getRight();
    if (matchingDocIds != null && matchingDocIds.isEmpty()) {
      return LazyBitmap.EMPTY_BITMAP;
    }

    Predicate.Type predicateType = predicate.getType();
    switch (predicateType) {
      case EQ: {
        String value = ((EqPredicate) predicate).getValue();
        String keyValuePair = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
        RoaringBitmap result = _postingListMap.get(keyValuePair);
        return filter(result, matchingDocIds);
      }

      case NOT_EQ: {
        String notEqualValue = ((NotEqPredicate) predicate).getValue();
        LazyBitmap result = null;

        RoaringBitmap allDocIds = _postingListMap.get(key);
        if (allDocIds != null && !allDocIds.isEmpty()) {
          result = new LazyBitmap(allDocIds);

          RoaringBitmap notEqualDocIds =
              _postingListMap.get(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + notEqualValue);

          if (notEqualDocIds != null && !notEqualDocIds.isEmpty()) {
            result = result.andNot(notEqualDocIds);
          }
        }

        return filter(result, matchingDocIds);
      }

      case IN: {
        List<String> values = ((InPredicate) predicate).getValues();
        LazyBitmap result = null;

        StringBuilder buffer = new StringBuilder(key);
        buffer.append(JsonIndexCreator.KEY_VALUE_SEPARATOR);
        int pos = buffer.length();

        for (String value : values) {
          buffer.setLength(pos);
          buffer.append(value);
          String keyValue = buffer.toString();

          RoaringBitmap docIds = _postingListMap.get(keyValue);

          if (docIds != null && !docIds.isEmpty()) {
            if (result == null) {
              result = new LazyBitmap(docIds);
            } else {
              result = result.or(docIds);
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case NOT_IN: {
        List<String> notInValues = ((NotInPredicate) predicate).getValues();
        LazyBitmap result = null;

        RoaringBitmap allDocIds = _postingListMap.get(key);
        if (allDocIds != null && !allDocIds.isEmpty()) {
          result = new LazyBitmap(allDocIds);

          StringBuilder buffer = new StringBuilder(key);
          buffer.append(JsonIndexCreator.KEY_VALUE_SEPARATOR);
          int pos = buffer.length();

          for (String notInValue : notInValues) {
            buffer.setLength(pos);
            buffer.append(notInValue);
            String keyValuePair = buffer.toString();

            RoaringBitmap docIds = _postingListMap.get(keyValuePair);
            if (docIds != null && !docIds.isEmpty()) {
              result = result.andNot(docIds);
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case IS_NOT_NULL:
      case IS_NULL: {
        RoaringBitmap result = _postingListMap.get(key);
        return filter(result, matchingDocIds);
      }

      case REGEXP_LIKE: {
        Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
        if (subMap.isEmpty()) {
          return LazyBitmap.EMPTY_BITMAP;
        }

        Pattern pattern = ((RegexpLikePredicate) predicate).getPattern();
        Matcher matcher = pattern.matcher("");
        LazyBitmap result = null;
        StringBuilder value = new StringBuilder();

        for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
          String keyValue = entry.getKey();
          value.setLength(0);
          value.append(keyValue, key.length() + 1, keyValue.length());

          if (!matcher.reset(value).matches()) {
            continue;
          }

          RoaringBitmap docIds = entry.getValue();
          if (docIds != null && !docIds.isEmpty()) {
            if (result == null) {
              result = new LazyBitmap(docIds);
            } else {
              result = result.or(docIds);
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case RANGE: {
        Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
        if (subMap.isEmpty()) {
          return LazyBitmap.EMPTY_BITMAP;
        }

        LazyBitmap result = null;
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
              result = new LazyBitmap(entry.getValue());
            } else {
              result = result.or(entry.getValue());
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      default:
        throw new IllegalStateException("Unsupported json_match predicate type: " + predicate);
    }
  }

  public void convertFlattenedDocIdsToDocIds(Map<String, RoaringBitmap> valueToFlattenedDocIds) {
    _readLock.lock();
    try {
      valueToFlattenedDocIds.replaceAll((key, value) -> {
        RoaringBitmap docIds = new RoaringBitmap();
        value.forEach((IntConsumer) flattenedDocId -> docIds.add(_docIdMapping.getInt(flattenedDocId)));
        return docIds;
      });
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public Map<String, RoaringBitmap> getMatchingFlattenedDocsMap(String jsonPathKey, @Nullable String filterString) {
    Map<String, RoaringBitmap> resultMap = new HashMap<>();
    _readLock.lock();
    try {
      LazyBitmap filteredDocIds = null;
      FilterContext filter;
      if (filterString != null) {
        filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
        Preconditions.checkArgument(!filter.isConstant(), "Invalid json match filter: " + filterString);

        if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
          // Handle exclusive predicate separately because the flip can only be applied to the
          // un-flattened doc ids in order to get the correct result, and it cannot be nested
          filteredDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
          filteredDocIds = filteredDocIds.flip(0, _nextFlattenedDocId);
        } else {
          filteredDocIds = getMatchingFlattenedDocIds(filter);
        }
      }
      // Support 2 formats:
      // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
      // - Legacy format (e.g. "a[1].b"='abc')
      if (jsonPathKey.startsWith("$")) {
        jsonPathKey = jsonPathKey.substring(1);
      } else {
        jsonPathKey = JsonUtils.KEY_SEPARATOR + jsonPathKey;
      }
      Pair<String, LazyBitmap> result = getKeyAndFlattenedDocIds(jsonPathKey);
      jsonPathKey = result.getLeft();
      LazyBitmap arrayIndexDocIds = result.getRight();
      if (arrayIndexDocIds != null && arrayIndexDocIds.isEmpty()) {
        return resultMap;
      }

      RoaringBitmap filteredBitmap = filteredDocIds != null ? filteredDocIds.getValue() : null;
      RoaringBitmap arrayIndexBitmap = arrayIndexDocIds != null ? arrayIndexDocIds.getValue() : null;

      Map<String, RoaringBitmap> subMap = getMatchingKeysMap(jsonPathKey);
      for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
        // there is no point using lazy bitmap here because filteredDocIds and arrayIndexDocIds
        // are shared and can't be modified
        RoaringBitmap docIds = entry.getValue();
        if (docIds == null || docIds.isEmpty()) {
          continue;
        }
        docIds = docIds.clone();
        if (filteredDocIds != null) {
          docIds.and(filteredBitmap);
        }
        if (arrayIndexDocIds != null) {
          docIds.and(arrayIndexBitmap);
        }

        if (!docIds.isEmpty()) {
          String value = entry.getKey().substring(jsonPathKey.length() + 1);
          resultMap.put(value, docIds);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(resultMap.size());
        }
      }

      return resultMap;
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
  private Pair<String, LazyBitmap> getKeyAndFlattenedDocIds(String key) {
    // Process the array index within the key if exists
    // E.g. "[*]"=1 -> "."='1'
    // E.g. "[0]"=1 -> ".$index"='0' && "."='1'
    // E.g. "[0][1]"=1 -> ".$index"='0' && "..$index"='1' && ".."='1'
    // E.g. ".foo[*].bar[*].foobar"='abc' -> ".foo..bar..foobar"='abc'
    // E.g. ".foo[0].bar[1].foobar"='abc' -> ".foo.$index"='0' && ".foo..bar.$index"='1' && ".foo..bar..foobar"='abc'
    // E.g. ".foo[0][1].bar"='abc' -> ".foo.$index"='0' && ".foo..$index"='1' && ".foo...bar"='abc'
    LazyBitmap matchingDocIds = null;
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
            matchingDocIds = new LazyBitmap(docIds);
          } else {
            matchingDocIds = matchingDocIds.and(docIds);
          }
        } else {
          return Pair.of(null, LazyBitmap.EMPTY_BITMAP);
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
  public String[][] getValuesMV(int[] docIds, int length,
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
  public String[] getValuesSV(int[] docIds, int length, Map<String, RoaringBitmap> valueToMatchingFlattenedDocs,
      boolean isFlattenedDocIds) {
    Int2ObjectOpenHashMap<String> docIdToValues = new Int2ObjectOpenHashMap<>(length);
    RoaringBitmap docIdMask = RoaringBitmap.bitmapOf(Arrays.copyOfRange(docIds, 0, length));
    _readLock.lock();
    try {
      for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
        String value = entry.getKey();
        RoaringBitmap matchingDocIds = entry.getValue();

        if (isFlattenedDocIds) {
          matchingDocIds.forEach((IntConsumer) flattenedDocId -> {
            int docId = _docIdMapping.getInt(flattenedDocId);
            if (docIdMask.contains(docId)) {
              docIdToValues.put(docId, value);
            }
          });
        } else {
          RoaringBitmap intersection = RoaringBitmap.and(entry.getValue(), docIdMask);
          if (intersection.isEmpty()) {
            continue;
          }
          for (int docId : intersection) {
            docIdToValues.put(docId, entry.getKey());
          }
        }
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

  // AND given bitmaps, optionally converting first one to mutable (if it's not already)
  private static LazyBitmap and(LazyBitmap target, LazyBitmap other) {
    if (target.isMutable()) {
      target.and(other);
      return target;
    } else if (other.isMutable()) {
      other.and(target);
      return other;
    } else {
      LazyBitmap mutableTarget = target.toMutable();
      mutableTarget.and(other);
      return mutableTarget;
    }
  }

  private static LazyBitmap and(LazyBitmap target, RoaringBitmap other) {
    if (target.isMutable()) {
      target.and(other);
      return target;
    } else {
      LazyBitmap mutableTarget = target.toMutable();
      mutableTarget.and(other);
      return mutableTarget;
    }
  }

  // OR given bitmaps, optionally converting first one to mutable (if it's not already)
  private static LazyBitmap or(LazyBitmap target, LazyBitmap other) {
    if (target.isMutable()) {
      target.or(other);
      return target;
    } else if (other.isMutable()) {
      other.or(target);
      return other;
    } else {
      LazyBitmap mutableTarget = target.toMutable();
      mutableTarget.or(other);
      return mutableTarget;
    }
  }

  private static LazyBitmap filter(LazyBitmap result, LazyBitmap matchingDocIds) {
    if (result == null) {
      return LazyBitmap.EMPTY_BITMAP;
    } else if (matchingDocIds == null) {
      return result;
    } else {
      return and(matchingDocIds, result);
    }
  }

  private static LazyBitmap filter(RoaringBitmap result, LazyBitmap matchingDocIds) {
    if (result == null) {
      return LazyBitmap.EMPTY_BITMAP;
    } else if (matchingDocIds == null) {
      return new LazyBitmap(result);
    } else {
      return and(matchingDocIds, result);
    }
  }
}
