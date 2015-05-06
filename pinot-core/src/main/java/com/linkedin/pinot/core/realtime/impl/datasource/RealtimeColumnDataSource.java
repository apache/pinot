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
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.index.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.index.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.MetricInvertedIndex;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RealtimeColumnDataSource implements DataSource {

  private static final int REALTIME_DICTIONARY_INIT_ID = 1;
  private Predicate predicate;

  private MutableRoaringBitmap filteredDocIdBitmap;

  private boolean blockReturned = false;
  private boolean isPredicateEvaluated = false;

  private final FieldSpec fieldSpec;
  private final DataFileReader indexReader;
  private final RealtimeInvertedIndex invertedIndex;
  private final int offset;
  private final int maxNumberOfMultiValues;
  private final MutableDictionaryReader dictionary;

  public RealtimeColumnDataSource(FieldSpec spec, DataFileReader indexReader, RealtimeInvertedIndex invertedIndex,
      int searchOffset, int maxNumberOfMultivalues, Schema schema, MutableDictionaryReader dictionary) {
    this.fieldSpec = spec;
    this.indexReader = indexReader;
    this.invertedIndex = invertedIndex;
    this.offset = searchOffset;
    this.maxNumberOfMultiValues = maxNumberOfMultivalues;
    this.dictionary = dictionary;
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
          if (invertedIndex != null) {
            evalPredicateWithDictAndInvIdx();
          } else {
            // Shouldn't hit here. Always create dictionary and inverted index together.
            throw new RuntimeException("Found column - " + fieldSpec.getName()
                + " has dictionary, but no inverted index.");
          }
        } else {
          evalPredicateNoDictHasInvIdx();
        }
        isPredicateEvaluated = true;
      }
      if (fieldSpec.isSingleValueField()) {
        Block SvBlock =
            new RealtimeSingleValueBlock(filteredDocIdBitmap, fieldSpec, dictionary, offset,
                (FixedByteSingleColumnSingleValueReaderWriter) indexReader);
        if (predicate != null) {
          SvBlock.applyPredicate(predicate);
        }
        return SvBlock;
      } else {
        Block mvBlock =
            new RealtimeMultiValueBlock(fieldSpec, dictionary, filteredDocIdBitmap, offset, maxNumberOfMultiValues,
                (FixedByteSingleColumnMultiValueReaderWriter) indexReader);
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
          filteredDocIdBitmap = invertedIndex.getDocIdSetFor(dictionary.indexOf(equalsValueToLookup));
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
            orBitmapForInQueries.or(invertedIndex.getDocIdSetFor(dictId));
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
            neqBitmap.or(invertedIndex.getDocIdSetFor(i));
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
            notINHolder.or(invertedIndex.getDocIdSetFor(i));
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
            rangeBitmap.or(invertedIndex.getDocIdSetFor(dicId));
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
        eqBitmapForInQueries = invertedIndex.getDocIdSetFor(getNumberObjectFromString(equalsValueToLookup));
        if (eqBitmapForInQueries == null) {
          eqBitmapForInQueries = new MutableRoaringBitmap();
        }
        filteredDocIdBitmap = eqBitmapForInQueries;
        break;
      case IN:
        MutableRoaringBitmap orBitmapForInQueries = new MutableRoaringBitmap();
        String[] inRangeStrings = ((InPredicate) predicate).getInRange();
        for (String rawValueInString : inRangeStrings) {
          MutableRoaringBitmap bitmap = invertedIndex.getDocIdSetFor(getNumberObjectFromString(rawValueInString));
          if (bitmap != null) {
            orBitmapForInQueries.or(bitmap);
          }
        }
        filteredDocIdBitmap = orBitmapForInQueries;
        break;
      case NEQ:
        MutableRoaringBitmap neqBitmap;
        String neqValue = ((NEqPredicate) predicate).getNotEqualsValue();
        neqBitmap = invertedIndex.getDocIdSetFor(getNumberObjectFromString(neqValue));
        if (neqBitmap == null) {
          neqBitmap = new MutableRoaringBitmap();
        }
        neqBitmap.flip(0, offset + 1);
        filteredDocIdBitmap = neqBitmap;
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();
        for (String notInValue : notInValues) {
          MutableRoaringBitmap notBitmap = invertedIndex.getDocIdSetFor(getNumberObjectFromString(notInValue));
          if (notBitmap != null) {
            notINHolder.or(notBitmap);
          }
        }
        notINHolder.flip(0, offset + 1);
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
        for (Object invKey : ((MetricInvertedIndex) invertedIndex).getKeys()) {
          double invKeyDouble = ((Number) invKey).doubleValue();
          if (rangeStart < invKeyDouble && invKeyDouble < rangeEnd) {
            rangeBitmap.or(invertedIndex.getDocIdSetFor(invKey));
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
    switch (fieldSpec.getDataType()) {
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
    throw new RuntimeException("Not support data type for column - " + fieldSpec.getDataType());
  }

  private double getLargerDoubleValue(double value) {
    long bitsValue = Double.doubleToLongBits(value);
    if (bitsValue >= 0) {
      return Double.longBitsToDouble(bitsValue + 1);
    }
    if (bitsValue == Long.MIN_VALUE) {
      return Double.longBitsToDouble(1L);
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
        return invertedIndex != null;
      }

      @Override
      public boolean hasDictionary() {
        return dictionary != null || getFieldType() != FieldType.METRIC;
      }

      @Override
      public FieldType getFieldType() {
        return fieldSpec.getFieldType();
      }

      @Override
      public DataType getDataType() {
        return fieldSpec.getDataType();
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
        return fieldSpec.isSingleValueField();
      }
    };
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return invertedIndex;
  }

  @Override
  public Dictionary getDictionary() {
    return dictionary;
  }
}
