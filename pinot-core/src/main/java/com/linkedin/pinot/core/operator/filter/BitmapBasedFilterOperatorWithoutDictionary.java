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
package com.linkedin.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.MetricInvertedIndex;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;


public class BitmapBasedFilterOperatorWithoutDictionary extends BaseFilterOperator {

  private static final Logger LOG = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);

  private DataSource dataSource;
  private BitmapBlock bitmapBlock;

  public BitmapBasedFilterOperatorWithoutDictionary(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean open() {
    return true;
  }

  private Object getNumberObjectFromString(String equalsValueToLookup) {
    switch (dataSource.getDataSourceMetadata().getDataType()) {
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
    throw new RuntimeException("Not support data type for column - " + dataSource.getDataSourceMetadata().getDataType());
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
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    RealtimeInvertedIndex invertedIndex = (RealtimeInvertedIndex) dataSource.getInvertedIndex();
    Block dataSourceBlock = dataSource.nextBlock();
    List<ImmutableRoaringBitmap> bitmapList = new ArrayList<ImmutableRoaringBitmap>();
    switch (predicate.getType()) {
      case EQ:
        MutableRoaringBitmap eqBitmapForInQueries;
        String equalsValueToLookup = ((EqPredicate) predicate).getEqualsValue();
        eqBitmapForInQueries = invertedIndex.getDocIdSetFor(getNumberObjectFromString(equalsValueToLookup));
        if (eqBitmapForInQueries != null) {
          bitmapList.add(eqBitmapForInQueries);
        }
        break;
      case NEQ:
        String neqValue = ((NEqPredicate) predicate).getNotEqualsValue();
        MutableRoaringBitmap neqBitmap = invertedIndex.getDocIdSetFor(getNumberObjectFromString(neqValue));
        if (neqBitmap == null) {
          neqBitmap = new MutableRoaringBitmap();
        }
        neqBitmap.flip(0, neqBitmap.getCardinality());
        bitmapList.add(neqBitmap);
        break;
      case IN:
        String[] inRangeStrings = ((InPredicate) predicate).getInRange();
        Set<String> inRangeStringSet = new HashSet<String>(Arrays.asList(inRangeStrings));
        for (String rawValueInString : inRangeStringSet) {
          MutableRoaringBitmap bitmap = invertedIndex.getDocIdSetFor(getNumberObjectFromString(rawValueInString));
          if (bitmap != null) {
            bitmapList.add(bitmap);
          }
        }
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
        notINHolder.flip(0, notINHolder.getCardinality());
        bitmapList.add(notINHolder);
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
        for (Object invKey : ((MetricInvertedIndex) invertedIndex).getKeys()) {
          double invKeyDouble = ((Number) invKey).doubleValue();
          if (rangeStart < invKeyDouble && invKeyDouble < rangeEnd) {
            bitmapList.add(invertedIndex.getDocIdSetFor(invKey));
          }
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("Regex not supported");
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[bitmapList.size()];
    bitmapList.toArray(bitmaps);
    bitmapBlock = new BitmapBlock(dataSourceBlock.getMetadata(), bitmaps);
    return bitmapBlock;
  }

  @Override
  public boolean close() {
    return true;
  }

  public static class BitmapBlock extends BaseFilterBlock {

    private final ImmutableRoaringBitmap[] bitmaps;
    private BitmapDocIdSet bitmapDocIdSet;
    private BlockMetadata blockMetadata;

    public BitmapBlock(BlockMetadata blockMetadata, ImmutableRoaringBitmap[] bitmaps) {
      this.blockMetadata = blockMetadata;
      this.bitmaps = bitmaps;
    }

    @Override
    public BlockId getId() {
      return new BlockId(0);
    }

    @Override
    public boolean applyPredicate(Predicate predicate) {
      throw new UnsupportedOperationException("applypredicate not supported in " + this.getClass());
    }

    @Override
    public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
      bitmapDocIdSet = new BitmapDocIdSet(blockMetadata, bitmaps);
      return bitmapDocIdSet;
    }

    @Override
    public BlockValSet getBlockValueSet() {
      throw new UnsupportedOperationException("getBlockValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockDocIdValueSet getBlockDocIdValueSet() {
      throw new UnsupportedOperationException("getBlockDocIdValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockMetadata getMetadata() {
      throw new UnsupportedOperationException("getMetadata not supported in " + this.getClass());
    }

  }

}
