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

import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;


/**
 * Filter operator for supporting the execution of text search
 * queries: WHERE TEXT_MATCH(column_name, query_string....)
 */
public class TextMatchFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "TextMatchFilterOperator";
  private static final String EXPLAIN_NAME = "FILTER_TEXT_INDEX";

  private final TextIndexReader _textIndexReader;
  private final int _numDocs;
  private final TextMatchPredicate _predicate;

  public TextMatchFilterOperator(TextIndexReader textIndexReader, TextMatchPredicate predicate, int numDocs) {
    _textIndexReader = textIndexReader;
    _predicate = predicate;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new BitmapDocIdSet(_textIndexReader.getDocIds(_predicate.getValue()), _numDocs));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getExplainPlanName()).append("(indexLookUp:text_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
