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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.realtime.utils.RealtimeMetricsSerDe;


public class RealtimeColumnDataSource implements DataSource {

  private static final int REALTIME_DICTIONARY_INIT_ID = 1;
  private final FieldSpec spec;
  private final MutableDictionaryReader dictionary;
  private final RealtimeInvertedIndex invertedINdex;
  private final String columnName;
  private final int docIdSearchableOffset;
  private final Schema schema;
  private final int maxNumberOfMultiValuesMap;
  private final RealtimeDimensionsSerDe dimSerDe;
  private final RealtimeMetricsSerDe metSerDe;
  private final ByteBuffer[] dimBuffs;
  private final ByteBuffer[] metBuffs;
  private final int[] time;
  private Predicate predicate;

  private MutableRoaringBitmap filteredDocIdBitmap;

  private boolean blockReturned = false;

  public RealtimeColumnDataSource(FieldSpec spec, MutableDictionaryReader dictionary,
      RealtimeInvertedIndex invertedIndex, String columnName, int docIdOffset, Schema schema,
      int maxNumberOfMultiValuesMap, RealtimeDimensionsSerDe dimSerDe, RealtimeMetricsSerDe metSerDe,
      ByteBuffer[] dims, ByteBuffer[] mets, int[] time) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.invertedINdex = invertedIndex;
    this.columnName = columnName;
    this.docIdSearchableOffset = docIdOffset;
    this.schema = schema;
    this.maxNumberOfMultiValuesMap = maxNumberOfMultiValuesMap;
    this.dimSerDe = dimSerDe;
    this.metSerDe = metSerDe;
    this.dimBuffs = dims;
    this.metBuffs = mets;
    this.time = time;
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
            new RealtimeSingleValueBlock(spec, dictionary, filteredDocIdBitmap, columnName, docIdSearchableOffset,
                schema, dimSerDe, metSerDe, dimBuffs, metBuffs, time);
        if (predicate != null) {
          SvBlock.applyPredicate(predicate);
        }
        return SvBlock;
      } else {
        Block mvBlock =
            new RealtimeMultivalueBlock(spec, dictionary, filteredDocIdBitmap, columnName, docIdSearchableOffset,
                schema, maxNumberOfMultiValuesMap, dimSerDe, dimBuffs);
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
        String equalsValueToLookup = ((EqPredicate) predicate).getEqualsValue();
        if (dictionary.contains(equalsValueToLookup)) {
          filteredDocIdBitmap = invertedINdex.getDocIdSetFor(dictionary.indexOf(equalsValueToLookup));
        } else {
          filteredDocIdBitmap = new MutableRoaringBitmap();
        }
        break;
      case IN:
        MutableRoaringBitmap orBitmapForInQueries = new MutableRoaringBitmap();
        String[] inRangeStrings = ((InPredicate) predicate).getInRange();
        for (String rawValueInString : inRangeStrings) {
          if (dictionary.contains(rawValueInString)) {
            int dictId = dictionary.indexOf(rawValueInString);
            orBitmapForInQueries.or(invertedINdex.getDocIdSetFor(dictId));
          }
        }
        filteredDocIdBitmap = orBitmapForInQueries;
        break;
      case NEQ:
        MutableRoaringBitmap neqBitmap = new MutableRoaringBitmap();
        String neqValue = ((NEqPredicate) predicate).getNotEqualsValue();
        int valueToExclude = -1;
        if (neqValue == null) {
          valueToExclude = 0;
        } else if (neqValue != null && dictionary.contains(neqValue)) {
          valueToExclude = dictionary.indexOf(neqValue);
        }

        for (int i = 1; i <= dictionary.length(); i++) {
          if (valueToExclude != i) {
            neqBitmap.or(invertedINdex.getDocIdSetFor(i));
          }
        }
        filteredDocIdBitmap = neqBitmap;
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final Set<Integer> notInIds = new HashSet<Integer>();

        for (final String notInValue : notInValues) {
          if (dictionary.contains(notInValue)) {
            notInIds.add(dictionary.indexOf(notInValue));
          }
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

        final boolean incLower = ((RangePredicate) predicate).includeLowerBoundary();
        final boolean incUpper = ((RangePredicate) predicate).includeUpperBoundary();
        final String lower = ((RangePredicate) predicate).getLowerBoundary();
        final String upper = ((RangePredicate) predicate).getUpperBoundary();

        if (lower.equals("*")) {
          rangeStart = dictionary.getStringValue(REALTIME_DICTIONARY_INIT_ID);
        } else {
          rangeStart = lower;
        }

        if (upper.equals("*")) {
          rangeEnd = dictionary.getStringValue(dictionary.length());
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
