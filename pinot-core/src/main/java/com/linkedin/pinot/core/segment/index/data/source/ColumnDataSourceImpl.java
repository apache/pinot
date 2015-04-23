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

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 15, 2014
 *
 */

public class ColumnDataSourceImpl implements DataSource {
  private static final Logger logger = Logger.getLogger(ColumnDataSourceImpl.class);

  private final ImmutableDictionaryReader dictionary;
  private final DataFileReader reader;
  private final BitmapInvertedIndex invertedIndex;
  private final ColumnMetadata columnMetadata;
  private Predicate predicate;
  private ImmutableRoaringBitmap filteredBitmap = null;
  private int blockNextCallCount = 0;
  boolean isPredicateEvaluated = false;

  public ColumnDataSourceImpl(ImmutableDictionaryReader dictionary, DataFileReader reader,
      BitmapInvertedIndex invertedIndex, ColumnMetadata columnMetadata) {
    this.dictionary = dictionary;
    this.reader = reader;
    this.invertedIndex = invertedIndex;
    this.columnMetadata = columnMetadata;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    if (!isPredicateEvaluated && predicate != null) {
      evalPredicate();
      isPredicateEvaluated = true;
    }
    blockNextCallCount++;
    if (blockNextCallCount <= 1) {
      if (columnMetadata.isSingleValue()) {
        return new SingleValueBlock(new BlockId(0), (FixedBitCompressedSVForwardIndexReader) reader, filteredBitmap,
            dictionary, columnMetadata);
      } else {
        return new MultiValueBlock(new BlockId(0), (FixedBitCompressedMVForwardIndexReader) reader, filteredBitmap,
            dictionary, columnMetadata);
      }
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId blockId) {
    if (!isPredicateEvaluated && predicate != null) {
      evalPredicate();
      isPredicateEvaluated = true;
    }
    if (columnMetadata.isSingleValue()) {
      return new SingleValueBlock(blockId, (FixedBitCompressedSVForwardIndexReader) reader, filteredBitmap, dictionary,
          columnMetadata);
    } else {
      return new MultiValueBlock(blockId, (FixedBitCompressedMVForwardIndexReader) reader, filteredBitmap, dictionary,
          columnMetadata);
    }
  }

  @Override
  public boolean close() {
    return true;
  }

  public ImmutableRoaringBitmap getFilteredBitmap() {
    return filteredBitmap;
  }

  @Override
  public boolean setPredicate(Predicate p) {
    predicate = p;
    return true;
  }

  private boolean evalPredicate() {
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
          filteredBitmap = new MutableRoaringBitmap();
          return true;
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
    return true;
  }
}
