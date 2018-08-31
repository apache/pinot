/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.operator.blocks.FilterBlock;
import com.linkedin.pinot.core.operator.docidsets.SizeBasedDocIdSet;


public class MatchAllFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "MatchEntireSegmentOperator";

  private int _maxDocId;

  public MatchAllFilterOperator(int totalDocs) {
    _maxDocId = totalDocs - 1;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new SizeBasedDocIdSet(_maxDocId));
  }

  @Override
  public boolean isResultEmpty() {
    return false;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
