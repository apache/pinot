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
import org.apache.lucene.search.ScoreDoc;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.LuceneIndexDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.TextMatchPredicateEvaluatorFactory;
import org.apache.pinot.core.segment.index.readers.LuceneTextIndexReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;

public class TextMatchFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "TextMatchFilterOperator";

  private final String _queryString;
  private final DataSource _dataSource;
  private final int _startDocId;
  private final int _endDocId;

  public TextMatchFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId, int endDocId) {
    Preconditions.checkArgument(predicateEvaluator instanceof TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator &&
    !predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());
    TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator evaluator = (TextMatchPredicateEvaluatorFactory.RawValueBasedTextMatchPredicateEvaluator)predicateEvaluator;
    _queryString = evaluator.getSearchQuery();
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected FilterBlock getNextBlock() {
    TextIndexReader<LuceneTextIndexReader.LuceneSearchResult> textIndexReader = _dataSource.getTextIndex();
    if (textIndexReader == null) {
      System.out.println("null reader");
    }
    Preconditions.checkState(textIndexReader != null, "Error: expecting text index for column");
    LuceneTextIndexReader.LuceneSearchResult luceneSearchResult = textIndexReader.search(_queryString);
    return new FilterBlock(new LuceneIndexDocIdSet(luceneSearchResult, _startDocId, _endDocId, textIndexReader));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
