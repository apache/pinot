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
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.MetricInvertedIndex;
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
  private boolean isPredicateEvaluated = false;

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
      if (!isPredicateEvaluated && predicate != null) {
        if (dictionary != null) {
          if (invertedINdex != null) {
            evalPredicateWithDictAndInvIdx();
          } else {
            // Shouldn't hit here. Always create dictionary and inverted index together.
            throw new RuntimeException("Found column - " + columnName + " has dictionary, but no inverted index.");
          }
        } else {
          if (invertedINdex != null) {
            evalPredicateNoDictHasInvIdx();
          } else {
            evalPredicateNoDictNoInvIdx();
          }
        }
        isPredicateEvaluated = true;
      }
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
    return true;
  }

  // For dimension and time column has both dictionary and inverted index.
  private boolean evalPredicateWithDictAndInvIdx() {
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

        for (int i = 1; i < dictionary.length(); i++) {
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

  // For metric column with inverted index.
  private boolean evalPredicateNoDictHasInvIdx() {
    switch (predicate.getType()) {
      case EQ:
        MutableRoaringBitmap eqBitmapForInQueries;
        String equalsValueToLookup = ((EqPredicate) predicate).getEqualsValue();
        eqBitmapForInQueries = invertedINdex.getDocIdSetFor(getNumberObjectFromString(equalsValueToLookup));
        if (eqBitmapForInQueries == null) {
          eqBitmapForInQueries = new MutableRoaringBitmap();
        }
        filteredDocIdBitmap = eqBitmapForInQueries;
        break;
      case IN:
        MutableRoaringBitmap orBitmapForInQueries = new MutableRoaringBitmap();
        String[] inRangeStrings = ((InPredicate) predicate).getInRange();
        for (String rawValueInString : inRangeStrings) {
          MutableRoaringBitmap bitmap = invertedINdex.getDocIdSetFor(getNumberObjectFromString(rawValueInString));
          if (bitmap != null) {
            orBitmapForInQueries.or(bitmap);
          }
        }
        filteredDocIdBitmap = orBitmapForInQueries;
        break;
      case NEQ:
        MutableRoaringBitmap neqBitmap;
        String neqValue = ((NEqPredicate) predicate).getNotEqualsValue();
        neqBitmap = invertedINdex.getDocIdSetFor(getNumberObjectFromString(neqValue));
        if (neqBitmap == null) {
          neqBitmap = new MutableRoaringBitmap();
        }
        neqBitmap.flip(0, docIdSearchableOffset + 1);
        filteredDocIdBitmap = neqBitmap;
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();
        for (String notInValue : notInValues) {
          MutableRoaringBitmap notBitmap = invertedINdex.getDocIdSetFor(getNumberObjectFromString(notInValue));
          if (notBitmap != null) {
            notINHolder.or(notBitmap);
          }
        }
        notINHolder.flip(0, docIdSearchableOffset + 1);
        filteredDocIdBitmap = notINHolder;
        break;
      case RANGE:
        double rangeStart = 0;
        double rangeEnd = 0;
        final boolean incLower = ((RangePredicate) predicate).includeLowerBoundary();
        final boolean incUpper = ((RangePredicate) predicate).includeUpperBoundary();
        final String lower = ((RangePredicate) predicate).getLowerBoundary();
        final String upper = ((RangePredicate) predicate).getUpperBoundary();
        if (lower.equals("*")) {
          rangeStart = Double.NEGATIVE_INFINITY;
        } else {
          rangeStart = Double.parseDouble(lower);
          if (incLower) {
            rangeStart = getSmallerDoubleValue(rangeStart);
          }
        }
        if (upper.equals("*")) {
          rangeEnd = Double.POSITIVE_INFINITY;
        } else {
          rangeEnd = Double.parseDouble(upper);
          if (incUpper) {
            rangeEnd = getLargerDoubleValue(rangeEnd);
          }
        }
        MutableRoaringBitmap rangeBitmap = new MutableRoaringBitmap();
        for (Object invKey : ((MetricInvertedIndex) invertedINdex).getKeys()) {
          double invKeyDouble = ((Number) invKey).doubleValue();
          if (rangeStart < invKeyDouble && invKeyDouble < rangeEnd) {
            rangeBitmap.or(invertedINdex.getDocIdSetFor(invKey));
          }
        }
        filteredDocIdBitmap = rangeBitmap;
        break;
      case REGEX:
        throw new UnsupportedOperationException("regex filter not supported");
    }
    return true;
  }

  private Object getNumberObjectFromString(String equalsValueToLookup) {
    switch (spec.getDataType()) {
      case INT:
        return Integer.valueOf(Integer.parseInt(equalsValueToLookup));
      case LONG:
        return Long.valueOf(Long.parseLong(equalsValueToLookup));
      case FLOAT:
        return Float.valueOf(Float.parseFloat(equalsValueToLookup));
      case DOUBLE:
        return Double.valueOf(Double.parseDouble(equalsValueToLookup));
      default:
        break;
    }
    throw new RuntimeException("Not support data type for column - " + spec.getDataType());
  }

  // For metric column without inverted index.
  private boolean evalPredicateNoDictNoInvIdx() {
    switch (predicate.getType()) {
      case EQ:
        double equalsValueToLookup = Double.parseDouble(((EqPredicate) predicate).getEqualsValue());
        filteredDocIdBitmap = new MutableRoaringBitmap();
        for (int i = 0; i <= docIdSearchableOffset; ++i) {
          if (Double.compare(((Number) metSerDe.getRawValueFor(columnName, metBuffs[i])).doubleValue(),
              equalsValueToLookup) == 0) {
            filteredDocIdBitmap.add(i);
          }
        }
        break;
      case IN:
        filteredDocIdBitmap = new MutableRoaringBitmap();
        String[] inRangeStrings = ((InPredicate) predicate).getInRange();
        Set<Double> inRangeDoubles = new HashSet<Double>(inRangeStrings.length);
        for (int i = 0; i < inRangeStrings.length; ++i) {
          inRangeDoubles.add(Double.parseDouble(inRangeStrings[i]));
        }
        for (int i = 0; i <= docIdSearchableOffset; ++i) {
          if (inRangeDoubles.contains(((Number) metSerDe.getRawValueFor(columnName, metBuffs[i])).doubleValue())) {
            filteredDocIdBitmap.add(i);
          }
        }
        break;
      case NEQ:
        double neqValue = Double.parseDouble(((NEqPredicate) predicate).getNotEqualsValue());
        filteredDocIdBitmap = new MutableRoaringBitmap();
        for (int i = 0; i <= docIdSearchableOffset; ++i) {
          if (Double.compare(((Number) metSerDe.getRawValueFor(columnName, metBuffs[i])).doubleValue(), neqValue) != 0) {
            filteredDocIdBitmap.add(i);
          }
        }
        break;
      case NOT_IN:
        filteredDocIdBitmap = new MutableRoaringBitmap();
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        Set<Double> notInDoubles = new HashSet<Double>(notInValues.length);
        for (int i = 0; i < notInValues.length; ++i) {
          notInDoubles.add(Double.parseDouble(notInValues[i]));
        }
        for (int i = 0; i <= docIdSearchableOffset; ++i) {
          if (!notInDoubles.contains(((Number) metSerDe.getRawValueFor(columnName, metBuffs[i])).doubleValue())) {
            filteredDocIdBitmap.add(i);
          }
        }
        break;
      case RANGE:
        double rangeStart = 0;
        double rangeEnd = 0;

        final boolean incLower = ((RangePredicate) predicate).includeLowerBoundary();
        final boolean incUpper = ((RangePredicate) predicate).includeUpperBoundary();
        final String lower = ((RangePredicate) predicate).getLowerBoundary();
        final String upper = ((RangePredicate) predicate).getUpperBoundary();

        if (lower.equals("*")) {
          rangeStart = Double.NEGATIVE_INFINITY;
        } else {
          rangeStart = Double.parseDouble(lower);
          if (incLower) {
            rangeStart = getSmallerDoubleValue(rangeStart);
          }
        }

        if (upper.equals("*")) {
          rangeEnd = Double.POSITIVE_INFINITY;
        } else {
          rangeEnd = Double.parseDouble(upper);
          if (incUpper) {
            rangeEnd = getLargerDoubleValue(rangeEnd);
          }
        }

        filteredDocIdBitmap = new MutableRoaringBitmap();
        for (int i = 0; i <= docIdSearchableOffset; ++i) {
          double val = ((Number) metSerDe.getRawValueFor(columnName, metBuffs[i])).doubleValue();
          if (val > rangeStart && val < rangeEnd) {
            filteredDocIdBitmap.add(i);
          }
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("regex filter not supported");
    }
    return true;
  }

  private double getLargerDoubleValue(double value) {
    long bitsValue = Double.doubleToLongBits(value);
    if (bitsValue >= 0) {
      return Double.longBitsToDouble(bitsValue + 1);
    }
    if (bitsValue == Long.MIN_VALUE) {
      return Double.longBitsToDouble(1);
    }
    return Double.longBitsToDouble(bitsValue - 1);

  }

  private double getSmallerDoubleValue(double value) {
    long bitsValue = Double.doubleToLongBits(value);
    if (bitsValue > 0) {
      return Double.longBitsToDouble(bitsValue - 1);
    }
    if (bitsValue == 0) {
      bitsValue = 1;
      return Double.longBitsToDouble(bitsValue) * -1;
    }
    return Double.longBitsToDouble(bitsValue + 1);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return new DataSourceMetadata() {

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public boolean hasInvertedIndex() {
        return invertedINdex != null;
      }

      @Override
      public boolean hasDictionary() {
        return dictionary != null;
      }

      @Override
      public FieldType getFieldType() {
        return spec.getFieldType();
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }

      @Override
      public int cardinality() {
        if (dictionary == null) {
          return Constants.EOF;
        }
        return dictionary.length();
      }

      @Override
      public boolean isSingleValue() {
        return spec.isSingleValueField();
      }
    };
  }
}
