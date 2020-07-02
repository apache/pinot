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

import com.google.common.base.Preconditions;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.TextMatchPredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.TextMatchPredicateEvaluatorFactory;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.text.LuceneTextIndexReader;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator for supporting the execution of text search
 * queries: WHERE TEXT_MATCH(column_name, query_string....)
 */
public class TextMatchFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "TextMatchFilterOperator";

  private final Predicate _predicate;
  private final DataSource _dataSource;
  private final int _startDocId;
  private final int _endDocId;

  public TextMatchFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId, int endDocId) {
    Preconditions.checkArgument(predicateEvaluator instanceof TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator &&
    !predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());
    TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator evaluator = (TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator)predicateEvaluator;
    _predicate = evaluator.getPredicate();
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected FilterBlock getNextBlock() {
    InvertedIndexReader textIndexReader = _dataSource.getInvertedIndex();
    Preconditions.checkNotNull(textIndexReader, "Error: expecting non-null text index");
    String searchQuery = ((TextMatchPredicate)_predicate).getSearchQuery();
    MutableRoaringBitmap docIds = (MutableRoaringBitmap) textIndexReader.getDocIds(searchQuery);
    return new FilterBlock(new BitmapDocIdSet(new ImmutableRoaringBitmap[]{docIds}, _startDocId, _endDocId, false));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
