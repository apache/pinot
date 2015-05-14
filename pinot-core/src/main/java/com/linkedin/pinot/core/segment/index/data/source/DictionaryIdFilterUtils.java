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
package com.linkedin.pinot.core.segment.index.data.source;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.filter.utils.RangePredicateEvaluator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class DictionaryIdFilterUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryIdFilterUtils.class);

  public static ImmutableRoaringBitmap filter2(Predicate predicate, InvertedIndexReader invertedIndex,
      Dictionary dictionary, ColumnMetadata columnMetadata) {
    ImmutableRoaringBitmap filteredBitmap = null;

    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(((EqPredicate) predicate).getEqualsValue());
        if (valueToLookUP < 0) {
          filteredBitmap = new MutableRoaringBitmap();
        } else {
          filteredBitmap = invertedIndex.getImmutable(valueToLookUP);
        }
        break;
      case NEQ:
        // will change this later
        final int neq = dictionary.indexOf(((NEqPredicate) predicate).getNotEqualsValue());
        final MutableRoaringBitmap holderNEQ = new MutableRoaringBitmap();
        for (int i = 0; i < dictionary.length(); i++) {
          if (i != neq) {
            holderNEQ.or(invertedIndex.getImmutable(i));
          }
        }
        filteredBitmap = holderNEQ;
        break;
      case IN:
        final String[] inValues = ((InPredicate) predicate).getInRange();
        final MutableRoaringBitmap inHolder = new MutableRoaringBitmap();

        for (final String value : inValues) {
          final int index = dictionary.indexOf(value);
          if (index >= 0) {
            inHolder.or(invertedIndex.getImmutable(index));
          }
        }
        filteredBitmap = inHolder;
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final List<Integer> notInIds = new ArrayList<Integer>();
        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            notINHolder.or(invertedIndex.getImmutable(i));
          }
        }

        filteredBitmap = notINHolder;
        break;
      case RANGE:

        int[] rangeStartEndIndex =
            RangePredicateEvaluator.get().evalStartEndIndex(dictionary, (RangePredicate) predicate);
        int rangeStartIndex = rangeStartEndIndex[0];
        int rangeEndIndex = rangeStartEndIndex[1];
        LOGGER.info("rangeStartIndex:{}, rangeEndIndex:{}", rangeStartIndex, rangeEndIndex);

        if (rangeStartIndex > rangeEndIndex) {
          filteredBitmap = new MutableRoaringBitmap();
        }

        final MutableRoaringBitmap rangeBitmapHolder = new MutableRoaringBitmap();
        for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
          rangeBitmapHolder.or(invertedIndex.getImmutable(i));
        }
        filteredBitmap = rangeBitmapHolder;
        break;
      case REGEX:
        throw new UnsupportedOperationException("unsupported type : " + columnMetadata.getDataType().toString()
            + " for filter type : regex");
    }

    return filteredBitmap;
  }

  public static List<Integer> filter(Predicate predicate, Dictionary dictionary) {

    List<Integer> ret = new ArrayList<Integer>();
    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(((EqPredicate) predicate).getEqualsValue());
        if (valueToLookUP >= 0) {
          ret.add(valueToLookUP);
        }
        break;
      case NEQ:
        // will change this later
        final int neq = dictionary.indexOf(((NEqPredicate) predicate).getNotEqualsValue());
        final MutableRoaringBitmap holderNEQ = new MutableRoaringBitmap();
        for (int i = 0; i < dictionary.length(); i++) {
          if (i != neq) {
            ret.add(i);
          }
        }
        break;
      case IN:
        final String[] inValues = ((InPredicate) predicate).getInRange();
        final MutableRoaringBitmap inHolder = new MutableRoaringBitmap();

        for (final String value : inValues) {
          final int index = dictionary.indexOf(value);
          if ((index >= 0) && (!ret.contains(index))) {
            ret.add(index);
          }
        }
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final List<Integer> notInIds = new ArrayList<Integer>();
        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            ret.add(i);
          }
        }

        break;
      case RANGE:

        int[] rangeStartEndIndex =
            RangePredicateEvaluator.get().evalStartEndIndex(dictionary, (RangePredicate) predicate);
        int rangeStartIndex = rangeStartEndIndex[0];
        int rangeEndIndex = rangeStartEndIndex[1];
        LOGGER.info("rangeStartIndex:{}, rangeEndIndex:{}", rangeStartIndex, rangeEndIndex);
        for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
          ret.add(i);
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("unsupported type : " + predicate.getType() + " : " + predicate);
    }
    return ret;
  }
}
