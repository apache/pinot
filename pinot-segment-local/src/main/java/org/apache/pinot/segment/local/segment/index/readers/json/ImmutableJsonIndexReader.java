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
package org.apache.pinot.segment.local.segment.index.readers.json;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Pattern;
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
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.BaseJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Reader for json index.
 */
public class ImmutableJsonIndexReader implements JsonIndexReader {
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  private final long _numDocs;
  private final int _version;
  private final StringDictionary _dictionary;
  private final BitmapInvertedIndexReader _invertedIndex;
  private final long _numFlattenedDocs;
  private final PinotDataBuffer _docIdMapping;

  public ImmutableJsonIndexReader(PinotDataBuffer dataBuffer, int numDocs) {
    _numDocs = numDocs;
    _version = dataBuffer.getInt(0);
    Preconditions.checkState(_version == BaseJsonIndexCreator.VERSION_1 || _version == BaseJsonIndexCreator.VERSION_2,
        "Unsupported json index version: %s", _version);

    int maxValueLength = dataBuffer.getInt(4);
    long dictionaryLength = dataBuffer.getLong(8);
    long invertedIndexLength = dataBuffer.getLong(16);
    long docIdMappingLength = dataBuffer.getLong(24);

    long dictionaryStartOffset = BaseJsonIndexCreator.HEADER_LENGTH;
    long dictionaryEndOffset = dictionaryStartOffset + dictionaryLength;
    _dictionary =
        new StringDictionary(dataBuffer.view(dictionaryStartOffset, dictionaryEndOffset, ByteOrder.BIG_ENDIAN), 0,
            maxValueLength);
    long invertedIndexEndOffset = dictionaryEndOffset + invertedIndexLength;
    _invertedIndex = new BitmapInvertedIndexReader(
        dataBuffer.view(dictionaryEndOffset, invertedIndexEndOffset, ByteOrder.BIG_ENDIAN), _dictionary.length());
    long docIdMappingEndOffset = invertedIndexEndOffset + docIdMappingLength;
    _numFlattenedDocs = (docIdMappingLength / Integer.BYTES);
    _docIdMapping = dataBuffer.view(invertedIndexEndOffset, docIdMappingEndOffset, ByteOrder.LITTLE_ENDIAN);
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

    if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
      // Handle exclusive predicate separately because the flip can only be applied to the unflattened doc ids in order
      // to get the correct result, and it cannot be nested
      MutableRoaringBitmap matchingFlattenedDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
      MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
      matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> matchingDocIds.add(getDocId(flattenedDocId)));
      matchingDocIds.flip(0, _numDocs);
      return matchingDocIds;
    } else {
      MutableRoaringBitmap matchingFlattenedDocIds = getMatchingFlattenedDocIds(filter);
      MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
      matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> matchingDocIds.add(getDocId(flattenedDocId)));
      return matchingDocIds;
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
  private MutableRoaringBitmap getMatchingFlattenedDocIds(FilterContext filter) {
    return getMatchingFlattenedDocIds(filter, false);
  }

  /**
   * When operator requires flattened doc IDs (eg: jsonExtractIndexArray), nested exclusive predicates are allowed.
   * When operator requires unflattened doc IDs, nested exclusive predicates are not allowed.
   */
  private MutableRoaringBitmap getMatchingFlattenedDocIds(FilterContext filter,
      boolean allowNestedExclusivePredicates) {
    switch (filter.getType()) {
      case AND: {
        List<FilterContext> children = filter.getChildren();
        int numChildren = children.size();
        MutableRoaringBitmap matchingDocIds =
            getMatchingFlattenedDocIds(children.get(0), allowNestedExclusivePredicates);
        for (int i = 1; i < numChildren; i++) {
          matchingDocIds.and(getMatchingFlattenedDocIds(children.get(i), allowNestedExclusivePredicates));
        }
        return matchingDocIds;
      }
      case OR: {
        List<FilterContext> children = filter.getChildren();
        int numChildren = children.size();
        MutableRoaringBitmap matchingDocIds =
            getMatchingFlattenedDocIds(children.get(0), allowNestedExclusivePredicates);
        for (int i = 1; i < numChildren; i++) {
          matchingDocIds.or(getMatchingFlattenedDocIds(children.get(i), allowNestedExclusivePredicates));
        }
        return matchingDocIds;
      }
      case PREDICATE: {
        Predicate predicate = filter.getPredicate();

        if (!allowNestedExclusivePredicates) {
          Preconditions.checkArgument(!isExclusive(predicate.getType()), "Exclusive predicate: %s cannot be nested",
              predicate);
        }

        MutableRoaringBitmap matchingFlattenedDocs = getMatchingFlattenedDocIds(predicate);
        if (isExclusive(predicate.getType())) {
          matchingFlattenedDocs.flip(0L, _numFlattenedDocs);
        }
        return matchingFlattenedDocs;
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
  private MutableRoaringBitmap getMatchingFlattenedDocIds(Predicate predicate) {
    ExpressionContext lhs = predicate.getLhs();
    Preconditions.checkArgument(lhs.getType() == ExpressionContext.Type.IDENTIFIER,
        "Left-hand side of the predicate must be an identifier, got: %s (%s). Put double quotes around the identifier"
            + " if needed.", lhs, lhs.getType());
    String key = lhs.getIdentifier();

    MutableRoaringBitmap matchingDocIds = null;
    if (_version == BaseJsonIndexCreator.VERSION_2) {
      // Support 2 formats:
      // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
      // - Legacy format (e.g. "a[1].b"='abc')
      if (key.charAt(0) == '$') {
        key = key.substring(1);
      } else {
        key = JsonUtils.KEY_SEPARATOR + key;
      }

      // Process the array index within the key if exists
      // E.g. "[*]"=1 -> "."='1'
      // E.g. "[0]"=1 -> ".$index"='0' && "."='1'
      // E.g. "[0][1]"=1 -> ".$index"='0' && "..$index"='1' && ".."='1'
      // E.g. ".foo[*].bar[*].foobar"='abc' -> ".foo..bar..foobar"='abc'
      // E.g. ".foo[0].bar[1].foobar"='abc' -> ".foo.$index"='0' && ".foo..bar.$index"='1' && ".foo..bar..foobar"='abc'
      // E.g. ".foo[0][1].bar"='abc' -> ".foo.$index"='0' && ".foo..$index"='1' && ".foo...bar"='abc'
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
          String searchKey =
              leftPart + JsonUtils.ARRAY_INDEX_KEY + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
          int dictId = _dictionary.indexOf(searchKey);
          if (dictId >= 0) {
            ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
            if (matchingDocIds == null) {
              matchingDocIds = docIds.toMutableRoaringBitmap();
            } else {
              matchingDocIds.and(docIds);
            }
          } else {
            return new MutableRoaringBitmap();
          }
        }

        key = leftPart + JsonUtils.KEY_SEPARATOR + rightPart;
      }
    } else {
      // For V1 backward-compatibility

      // Support 2 formats:
      // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
      // - Legacy format (e.g. "a[1].b"='abc')
      if (key.startsWith("$.")) {
        key = key.substring(2);
      }

      // Process the array index within the key if exists
      // E.g. "foo[0].bar[1].foobar"='abc' -> "foo.$index"=0 && "foo.bar.$index"=1 && "foo.bar.foobar"='abc'
      int leftBracketIndex;
      while ((leftBracketIndex = key.indexOf('[')) > 0) {
        int rightBracketIndex = key.indexOf(']', leftBracketIndex + 2);
        Preconditions.checkArgument(rightBracketIndex > 0, "Missing right bracket in key: %s", key);

        String leftPart = key.substring(0, leftBracketIndex);
        String arrayIndex = key.substring(leftBracketIndex + 1, rightBracketIndex);
        String rightPart = key.substring(rightBracketIndex + 1);

        if (!arrayIndex.equals(JsonUtils.WILDCARD)) {
          // "foo[1].bar"='abc' -> "foo.$index"=1 && "foo.bar"='abc'
          String searchKey =
              leftPart + JsonUtils.ARRAY_INDEX_KEY + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
          int dictId = _dictionary.indexOf(searchKey);
          if (dictId >= 0) {
            ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
            if (matchingDocIds == null) {
              matchingDocIds = docIds.toMutableRoaringBitmap();
            } else {
              matchingDocIds.and(docIds);
            }
          } else {
            return new MutableRoaringBitmap();
          }
        }

        key = leftPart + rightPart;
      }
    }

    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ || predicateType == Predicate.Type.NOT_EQ) {
      String value = predicateType == Predicate.Type.EQ ? ((EqPredicate) predicate).getValue()
          : ((NotEqPredicate) predicate).getValue();
      String keyValuePair = key + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + value;
      int dictId = _dictionary.indexOf(keyValuePair);
      if (dictId >= 0) {
        ImmutableRoaringBitmap matchingDocIdsForKeyValuePair = _invertedIndex.getDocIds(dictId);
        if (matchingDocIds == null) {
          matchingDocIds = matchingDocIdsForKeyValuePair.toMutableRoaringBitmap();
        } else {
          matchingDocIds.and(matchingDocIdsForKeyValuePair);
        }
        return matchingDocIds;
      } else {
        return new MutableRoaringBitmap();
      }
    } else if (predicateType == Predicate.Type.IN || predicateType == Predicate.Type.NOT_IN) {
      List<String> values = predicateType == Predicate.Type.IN ? ((InPredicate) predicate).getValues()
          : ((NotInPredicate) predicate).getValues();
      MutableRoaringBitmap matchingDocIdsForKeyValuePairs = new MutableRoaringBitmap();
      for (String value : values) {
        String keyValuePair = key + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + value;
        int dictId = _dictionary.indexOf(keyValuePair);
        if (dictId >= 0) {
          matchingDocIdsForKeyValuePairs.or(_invertedIndex.getDocIds(dictId));
        }
      }
      if (matchingDocIds == null) {
        matchingDocIds = matchingDocIdsForKeyValuePairs;
      } else {
        matchingDocIds.and(matchingDocIdsForKeyValuePairs);
      }
      return matchingDocIds;
    } else if (predicateType == Predicate.Type.IS_NOT_NULL || predicateType == Predicate.Type.IS_NULL) {
      int dictId = _dictionary.indexOf(key);
      if (dictId >= 0) {
        ImmutableRoaringBitmap matchingDocIdsForKey = _invertedIndex.getDocIds(dictId);
        if (matchingDocIds == null) {
          matchingDocIds = matchingDocIdsForKey.toMutableRoaringBitmap();
        } else {
          matchingDocIds.and(matchingDocIdsForKey);
        }
        return matchingDocIds;
      } else {
        return new MutableRoaringBitmap();
      }
    } else if (predicateType == Predicate.Type.REGEXP_LIKE) {
      Pattern pattern = ((RegexpLikePredicate) predicate).getPattern();
      int[] dictIds = getDictIdRangeForKey(key);

      MutableRoaringBitmap result = null;
      for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
        String value = _dictionary.getStringValue(dictId).substring(key.length() + 1);
        if (pattern.matcher(value).matches()) {
          if (result == null) {
            result = _invertedIndex.getDocIds(dictId).toMutableRoaringBitmap();
          } else {
            result.or(_invertedIndex.getDocIds(dictId));
          }
        }
      }
      if (result == null) {
        return new MutableRoaringBitmap();
      } else {
        if (matchingDocIds == null) {
          return result;
        } else {
          matchingDocIds.and(result);
          return matchingDocIds;
        }
      }
    } else if (predicateType == Predicate.Type.RANGE) {
      RangePredicate rangePredicate = (RangePredicate) predicate;
      FieldSpec.DataType rangeDataType = rangePredicate.getRangeDataType();
      boolean lowerUnbounded = rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED);
      boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
      Object lowerBound = lowerUnbounded ? null : rangeDataType.convert(rangePredicate.getLowerBound());
      Object upperBound = upperUnbounded ? null : rangeDataType.convert(rangePredicate.getUpperBound());

      int[] dictIds = getDictIdRangeForKey(key);
      MutableRoaringBitmap result = null;
      for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
        String value = _dictionary.getStringValue(dictId).substring(key.length() + 1);
        Object valueObj = rangeDataType.convert(value);
        boolean lowerCompareResult =
            lowerUnbounded || (lowerInclusive ? rangeDataType.compare(valueObj, lowerBound) >= 0
                : rangeDataType.compare(valueObj, lowerBound) > 0);
        boolean upperCompareResult =
            upperUnbounded || (upperInclusive ? rangeDataType.compare(valueObj, upperBound) <= 0
                : rangeDataType.compare(valueObj, upperBound) < 0);

        if (lowerCompareResult && upperCompareResult) {
          if (result == null) {
            result = _invertedIndex.getDocIds(dictId).toMutableRoaringBitmap();
          } else {
            result.or(_invertedIndex.getDocIds(dictId));
          }
        }
      }

      if (result == null) {
        return new MutableRoaringBitmap();
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

  private int getDocId(int flattenedDocId) {
    return _docIdMapping.getInt((long) flattenedDocId << 2);
  }

  @Override
  public Map<String, RoaringBitmap> getMatchingDocsMap(String key) {
    Map<String, RoaringBitmap> matchingDocsMap = new HashMap<>();
    int[] dictIds = getDictIdRangeForKey(key);

    for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
      // get docIds from posting list, convert these to the actual docIds
      ImmutableRoaringBitmap flattenedDocIds = _invertedIndex.getDocIds(dictId);
      PeekableIntIterator it = flattenedDocIds.getIntIterator();
      RoaringBitmap realDocIds = new RoaringBitmap();
      while (it.hasNext()) {
        realDocIds.add(getDocId(it.next()));
      }
      matchingDocsMap.put(_dictionary.getStringValue(dictId).substring(key.length() + 1), realDocIds);
    }

    return matchingDocsMap;
  }

  @Override
  public Map<String, ImmutableRoaringBitmap> getValueToFlattenedDocIdsMap(String jsonPathKey,
      @Nullable String filterString) {
    ImmutableRoaringBitmap filteredFlattenedDocIds = null;
    if (filterString != null) {
      FilterContext filter;
      try {
        filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
        Preconditions.checkArgument(!filter.isConstant());
      } catch (Exception e) {
        throw new BadQueryRequestException("Invalid json match filter: " + filterString);
      }
      filteredFlattenedDocIds = getMatchingFlattenedDocIds(filter, true).toImmutableRoaringBitmap();
    }
    Map<String, ImmutableRoaringBitmap> result = new HashMap<>();
    int[] dictIds = getDictIdRangeForKey(jsonPathKey);
    for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
      String key = _dictionary.getStringValue(dictId);
      MutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId).toMutableRoaringBitmap();
      if (filteredFlattenedDocIds != null) {
        docIds.and(filteredFlattenedDocIds);
      }
      result.put(key.substring(jsonPathKey.length() + 1), docIds.toImmutableRoaringBitmap());
    }

    return result;
  }

  @Override
  public String[][] getValuesForMv(int[] docIds, int length,
      Map<String, ImmutableRoaringBitmap> valueToMatchingFlattenedDocs) {
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

    for (Map.Entry<String, ImmutableRoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
      String value = entry.getKey();
      ImmutableRoaringBitmap matchingFlattenedDocIds = entry.getValue();
      matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> {
        int docId = getDocId(flattenedDocId);
        if (docIdToPos.containsKey(docId)) {
          docIdToFlattenedDocIdsAndValues.get(docIdToPos.get(docId)).add(Pair.of(value, flattenedDocId));
        }
      });
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
  public String[] getValuesForKeyAndDocs(int[] docIds, Map<String, RoaringBitmap> matchingDocsMap) {
    Int2ObjectOpenHashMap<String> docIdToValues = new Int2ObjectOpenHashMap<>(docIds.length);
    RoaringBitmap docIdMask = RoaringBitmap.bitmapOf(docIds);

    for (Map.Entry<String, RoaringBitmap> entry : matchingDocsMap.entrySet()) {
      RoaringBitmap intersection = RoaringBitmap.and(entry.getValue(), docIdMask);
      if (intersection.isEmpty()) {
        continue;
      }
      for (int docId : intersection) {
        docIdToValues.put(docId, entry.getKey());
      }
    }

    String[] values = new String[docIds.length];
    for (int i = 0; i < docIds.length; i++) {
      values[i] = docIdToValues.get(docIds[i]);
    }
    return values;
  }

  /**
   * For a JSON key path, returns an int array of the range [min, max] spanning all values for the JSON key path
   */
  private int[] getDictIdRangeForKey(String key) {
    // json_index uses \0 as the separator (or \u0000 in unicode)
    // therefore, use the unicode char \u0001 to get the range of dict entries that have this prefix

    // get min for key
    int indexOfMin = _dictionary.indexOf(key);
    if (indexOfMin == -1) {
      return new int[]{-1, -1}; // if key does not exist, immediately return
    }
    int indexOfMax = _dictionary.insertionIndexOf(key + "\u0001");

    int minDictId = indexOfMin + 1; // skip the index of the key only
    int maxDictId = -1 * indexOfMax - 1; // undo the binary search
    if (indexOfMax > 0) {
      maxDictId = indexOfMax;
    }

    return new int[]{minDictId, maxDictId};
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
