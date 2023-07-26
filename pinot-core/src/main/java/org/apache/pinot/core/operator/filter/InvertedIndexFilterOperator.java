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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.EmptyFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class InvertedIndexFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_INVERTED_INDEX";

  private final PredicateEvaluator _predicateEvaluator;
  private final InvertedIndexReader<ImmutableRoaringBitmap> _invertedIndexReader;
  private final boolean _exclusive;
  private final int _numDocs;

  InvertedIndexFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    @SuppressWarnings("unchecked")
    InvertedIndexReader<ImmutableRoaringBitmap> invertedIndexReader =
        (InvertedIndexReader<ImmutableRoaringBitmap>) dataSource.getInvertedIndex();
    _invertedIndexReader = invertedIndexReader;
    _exclusive = predicateEvaluator.isExclusive();
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    int numDictIds = dictIds.length;
    if (numDictIds == 0) {
      return EmptyFilterBlock.getInstance();
    }
    if (numDictIds == 1) {
      ImmutableRoaringBitmap docIds = _invertedIndexReader.getDocIds(dictIds[0]);
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
        bitmaps[i] = _invertedIndexReader.getDocIds(dictIds[i]);
      }
      MutableRoaringBitmap docIds = ImmutableRoaringBitmap.or(bitmaps);
      if (_exclusive) {
        docIds.flip(0L, _numDocs);
      }
      InvocationRecording recording = Tracing.activeRecording();
      if (recording.isEnabled()) {
        recording.setColumnName(_predicateEvaluator.getPredicate().getLhs().getIdentifier());
        recording.setNumDocsMatchingAfterFilter(docIds.getCardinality());
        recording.setFilter(FilterType.INDEX, String.valueOf(_predicateEvaluator.getPredicateType()));
      }
      return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs));
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    int count = 0;
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    switch (dictIds.length) {
      case 0:
        break;
      case 1: {
        count = _invertedIndexReader.getDocIds(dictIds[0]).getCardinality();
        break;
      }
      case 2: {
        count = ImmutableRoaringBitmap.orCardinality(_invertedIndexReader.getDocIds(dictIds[0]),
            _invertedIndexReader.getDocIds(dictIds[1]));
        break;
      }
      default: {
        // this could be optimised if the bitmaps are known to be disjoint (as in a single value bitmap index)
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int dictId : dictIds) {
          bitmap.or(_invertedIndexReader.getDocIds(dictId));
        }
        count = bitmap.getCardinality();
        break;
      }
    }
    return _exclusive ? _numDocs - count : count;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      bitmaps[i] = _invertedIndexReader.getDocIds(dictIds[i]);
    }
    return new BitmapCollection(_numDocs, _exclusive, bitmaps);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:inverted_index");
    Predicate predicate = _predicateEvaluator.getPredicate();
    stringBuilder.append(",operator:").append(predicate.getType());
    stringBuilder.append(",predicate:").append(predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
