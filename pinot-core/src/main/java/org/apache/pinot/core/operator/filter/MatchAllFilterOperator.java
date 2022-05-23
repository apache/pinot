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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;


public class MatchAllFilterOperator extends BaseFilterOperator {
  public static final String EXPLAIN_NAME = "FILTER_MATCH_ENTIRE_SEGMENT";
  private final int _numDocs;

  public MatchAllFilterOperator(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public final boolean isResultMatchingAll() {
    return true;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new MatchAllDocIdSet(_numDocs));
  }


  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _numDocs;
  }

  @Override
  public String toExplainString() {
    return new StringBuilder(EXPLAIN_NAME).append("(docs:").append(_numDocs).append(')').toString();
  }
}
