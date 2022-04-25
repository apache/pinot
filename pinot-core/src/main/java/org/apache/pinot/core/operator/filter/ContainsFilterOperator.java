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
import org.apache.pinot.common.request.context.predicate.ContainsPredicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;


/**
 * Operator for CONTAINS query
 */
public class ContainsFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "ContainsFilterOperator";
  private static final String EXPLAIN_NAME = "FILTER_CONTAINS";

  private final NativeTextIndexReader _textIndexReader;
  private final int _numDocs;
  private final ContainsPredicate _predicate;

  public ContainsFilterOperator(NativeTextIndexReader textIndexReader, ContainsPredicate predicate, int numDocs) {
    _textIndexReader = textIndexReader;
    _predicate = predicate;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new BitmapDocIdSet(_textIndexReader.getDocIds(_predicate.getValue()), _numDocs));
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _textIndexReader.getDocIds(_predicate.getValue()).getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, false, _textIndexReader.getDocIds(_predicate.getValue()));
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:contains");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
