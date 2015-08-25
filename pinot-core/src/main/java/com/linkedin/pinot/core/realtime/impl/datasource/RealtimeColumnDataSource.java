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
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.index.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.index.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RealtimeColumnDataSource extends DataSource {

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
      isPredicateEvaluated = true;
      if (fieldSpec.isSingleValueField()) {
        Block SvBlock =
            new RealtimeSingleValueBlock(filteredDocIdBitmap, fieldSpec, dictionary, offset,
                (FixedByteSingleColumnSingleValueReaderWriter) indexReader);
        return SvBlock;
      } else {
        Block mvBlock =
            new RealtimeMultiValueBlock(fieldSpec, dictionary, filteredDocIdBitmap, offset, maxNumberOfMultiValues,
                (FixedByteSingleColumnMultiValueReaderWriter) indexReader);
        return mvBlock;
      }
    }
    return null;
  }

  @Override
  public Block getNextBlock() {
    return getBlock();
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    if (BlockId.getId() == 0) {
      blockReturned = false;
    }
    return getBlock();
  }

  @Override
  public String getOperatorName() {
    return "RealtimeColumnDataSource";
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    throw new UnsupportedOperationException("cannot set predicate on the data source");
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
        return dictionary != null;
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
