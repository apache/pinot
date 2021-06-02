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
package org.apache.pinot.core.operator.filter;

import org.apache.pinot.core.operator.blocks.EmptyFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@SuppressWarnings("rawtypes")
public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "BitmapBasedFilterOperator";

  private final PredicateEvaluator _predicateEvaluator;
  private final InvertedIndexReader _invertedIndexReader;
  private final ImmutableRoaringBitmap _docIds;
  private final boolean _exclusive;
  private final int _numDocs;

  BitmapBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _invertedIndexReader = dataSource.getInvertedIndex();
    _docIds = null;
    _exclusive = predicateEvaluator.isExclusive();
    _numDocs = numDocs;
  }

  public BitmapBasedFilterOperator(ImmutableRoaringBitmap docIds, boolean exclusive, int numDocs) {
    _predicateEvaluator = null;
    _invertedIndexReader = null;
    _docIds = docIds;
    _exclusive = exclusive;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_docIds != null) {
      if (_exclusive) {
        return new FilterBlock(new BitmapDocIdSet(ImmutableRoaringBitmap.flip(_docIds, 0L, _numDocs), _numDocs));
      } else {
        return new FilterBlock(new BitmapDocIdSet(_docIds, _numDocs));
      }
    }

    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    int numDictIds = dictIds.length;
    if (numDictIds == 0) {
      return EmptyFilterBlock.getInstance();
    }
    if (numDictIds == 1) {
      ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) _invertedIndexReader.getDocIds(dictIds[0]);
      if (_exclusive) {
        if (docIds instanceof MutableRoaringBitmap) {
          MutableRoaringBitmap mutableRoaringBitmap = (MutableRoaringBitmap) docIds;
          mutableRoaringBitmap.flip(0L, _numDocs);
          return new FilterBlock(new BitmapDocIdSet(mutableRoaringBitmap, _numDocs));
        } else {
          return new FilterBlock(new BitmapDocIdSet(ImmutableRoaringBitmap.flip(docIds, 0L, _numDocs), _numDocs));
        }
      } else {
        return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs));
      }
    } else {
      ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numDictIds];
      for (int i = 0; i < numDictIds; i++) {
        bitmaps[i] = (ImmutableRoaringBitmap) _invertedIndexReader.getDocIds(dictIds[i]);
      }
      MutableRoaringBitmap docIds = ImmutableRoaringBitmap.or(bitmaps);
      if (_exclusive) {
        docIds.flip(0L, _numDocs);
      }
      return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs));
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
