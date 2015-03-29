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
package com.linkedin.pinot.core.realtime.impl.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.fwdindex.DimensionTuple;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.realtime.utils.RealtimeMetricsSerDe;


public class RealtimeColumnDataSource implements DataSource {

  private static final int REALTIME_DICTIONARY_INIT_ID = 1;
  private final FieldSpec spec;
  private final MutableDictionaryReader dictionary;
  private final Map<Object, Pair<Long, Object>> docIdMap;
  private final RealtimeInvertedIndex invertedINdex;
  private final String columnName;
  private final int docIdSearchableOffset;
  private final Schema schema;
  private final Map<Long, DimensionTuple> dimensionTupleMap;
  private final int maxNumberOfMultiValuesMap;
  private final RealtimeDimensionsSerDe dimSerDe;
  private final RealtimeMetricsSerDe metSerDe;

  private Predicate predicate;

  private MutableRoaringBitmap filteredDocIdBitmap;

  private boolean blockReturned = false;

  public RealtimeColumnDataSource(FieldSpec spec, MutableDictionaryReader dictionary,
      Map<Object, Pair<Long, Object>> docIdMap, RealtimeInvertedIndex invertedIndex, String columnName,
      int docIdOffset, Schema schema, Map<Long, DimensionTuple> dimensionTupleMap, int maxNumberOfMultiValuesMap,
      RealtimeDimensionsSerDe dimSerDe, RealtimeMetricsSerDe metSerDe) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.docIdMap = docIdMap;
    this.invertedINdex = invertedIndex;
    this.columnName = columnName;
    this.docIdSearchableOffset = docIdOffset;
    this.schema = schema;
    this.dimensionTupleMap = dimensionTupleMap;
    this.maxNumberOfMultiValuesMap = maxNumberOfMultiValuesMap;
    this.dimSerDe = dimSerDe;
    this.metSerDe = metSerDe;
  }

  @Override
  public boolean open() {
    return true;
  }

  private Block getBlock() {
    if (!blockReturned) {
      blockReturned = true;
      if (spec.isSingleValueField()) {
        Block SvBlock =
            new RealtimeSingleValueBlock(spec, dictionary, docIdMap, filteredDocIdBitmap, columnName,
                docIdSearchableOffset, schema, dimensionTupleMap, dimSerDe, metSerDe);
        if (predicate != null) {
          SvBlock.applyPredicate(predicate);
        }
        return SvBlock;
      } else {
        Block mvBlock =
            new RealtimeMultivalueBlock(spec, dictionary, docIdMap, filteredDocIdBitmap, columnName,
                docIdSearchableOffset, schema, dimensionTupleMap, maxNumberOfMultiValuesMap, dimSerDe);
        if (predicate != null) {
          mvBlock.applyPredicate(predicate);
        }
        return mvBlock;
      }
    }
    return null;
  }

  @Override
  public Block nextBlock() {
    return getBlock();
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    if (BlockId.getId() == 0) {
      blockReturned = false;
    }
    return getBlock();
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    this.predicate = predicate;
    switch (predicate.getType()) {
      case EQ:
        String equalsValueToLookup = predicate.getRhs().get(0);
        filteredDocIdBitmap = invertedINdex.getDocIdSetFor(dictionary.indexOf(equalsValueToLookup));
        break;
      case IN:
        MutableRoaringBitmap orBitmapForInQueries = new MutableRoaringBitmap();
        int[] dicIdsToOrTogether = new int[predicate.getRhs().get(0).split(",").length];
        int counter = 0;
        for (String rawValueInString : predicate.getRhs().get(0).split(",")) {
          dicIdsToOrTogether[counter++] = dictionary.indexOf(rawValueInString);
        }
        for (int dicId : dicIdsToOrTogether) {
          orBitmapForInQueries.or(invertedINdex.getDocIdSetFor(dicId));
        }
        filteredDocIdBitmap = orBitmapForInQueries;
        break;
      case NEQ:
        MutableRoaringBitmap neqBitmap = new MutableRoaringBitmap();
        int valueToExclude = predicate.getRhs().get(0) == null ? 0 : dictionary.indexOf(predicate.getRhs().get(0));

        for (int i = 1; i <= dictionary.length(); i++) {
          if (valueToExclude != i) {
            neqBitmap.or(invertedINdex.getDocIdSetFor(i));
          }
        }
        filteredDocIdBitmap = neqBitmap;
        break;
      case NOT_IN:
        final String[] notInValues = predicate.getRhs().get(0).split(",");
        final List<Integer> notInIds = new ArrayList<Integer>();

        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            notINHolder.or(invertedINdex.getDocIdSetFor(i));
          }
        }
        filteredDocIdBitmap = notINHolder;
        break;
      case RANGE:
        String rangeStart = "";
        String rangeEnd = "";

        final String rangeString = predicate.getRhs().get(0);
        boolean incLower = true;
        boolean incUpper = true;

        if (rangeString.trim().startsWith("(")) {
          incLower = false;
        }

        if (rangeString.trim().endsWith(")")) {
          incUpper = false;
        }

        final String lower = rangeString.split(",")[0].substring(1, rangeString.split(",")[0].length());
        final String upper = rangeString.split(",")[1].substring(0, rangeString.split(",")[1].length() - 1);

        if (lower.equals("*")) {
          rangeStart = dictionary.getString(REALTIME_DICTIONARY_INIT_ID);
          incLower = true;
        } else {
          rangeStart = lower;
        }

        if (upper.equals("*")) {
          rangeEnd = dictionary.getString(dictionary.length());
          incUpper = true;
        } else {
          rangeEnd = upper;
        }

        MutableRoaringBitmap rangeBitmap = new MutableRoaringBitmap();
        for (int dicId = 1; dicId <= dictionary.length(); dicId++) {
          if (dictionary.inRange(rangeStart, rangeEnd, dicId, incLower, incUpper)) {
            rangeBitmap.or(invertedINdex.getDocIdSetFor(dicId));
          }
        }

        filteredDocIdBitmap = rangeBitmap;
        break;
      case REGEX:
        throw new UnsupportedOperationException("regex filter not supported");
    }
    return true;
  }

}
