package com.linkedin.pinot.core.segment.index.data.source;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class DictionaryIdFilterUtils {

  public static List<Integer> filter(Predicate predicate, Dictionary dictionary) {

    List<Integer> ret = new ArrayList<Integer>();
    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(((EqPredicate) predicate).getEqualsValue());
        ret.add(valueToLookUP);
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
          if (index >= 0) {
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

        int rangeStartIndex = 0;
        int rangeEndIndex = 0;

        final boolean incLower = ((RangePredicate) predicate).includeLowerBoundary();
        final boolean incUpper = ((RangePredicate) predicate).includeUpperBoundary();
        final String lower = ((RangePredicate) predicate).getLowerBoundary();
        final String upper = ((RangePredicate) predicate).getUpperBoundary();

        if (lower.equals("*")) {
          rangeStartIndex = 0;
        } else {
          rangeStartIndex = dictionary.indexOf(lower);
        }

        if (upper.equals("*")) {
          rangeEndIndex = dictionary.length() - 1;
        } else {
          rangeEndIndex = dictionary.indexOf(upper);
        }
        if (rangeStartIndex < 0) {
          rangeStartIndex = -(rangeStartIndex + 1);
        } else if (!incLower && !lower.equals("*")) {
          rangeStartIndex += 1;
        }

        if (rangeEndIndex < 0) {
          rangeEndIndex = -(rangeEndIndex + 1);
          rangeEndIndex = Math.max(0, rangeEndIndex - 1);
        } else if (!incUpper && !upper.equals("*")) {
          rangeEndIndex -= 1;
        }

        if (rangeStartIndex > rangeEndIndex) {

        } else {
          for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
            ret.add(i);
          }
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("unsupported type : " + predicate.getType() + " : " + predicate);
    }
    return ret;
  }
}
