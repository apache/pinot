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

import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;


/**
 * Filter operator for JSON_MATCH. E.g. SELECT ... WHERE JSON_MATCH(column_name, filter_string)
 */
public class JsonMatchFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "JsonMatchFilterOperator";
  private static final String EXPLAIN_NAME = "FILTER_JSON_INDEX";

  private final JsonIndexReader _jsonIndex;
  private final int _numDocs;
  private final JsonMatchPredicate _predicate;

  public JsonMatchFilterOperator(JsonIndexReader jsonIndex, JsonMatchPredicate predicate,
      int numDocs) {
    _jsonIndex = jsonIndex;
    _predicate = predicate;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new BitmapDocIdSet(_jsonIndex.getMatchingDocIds(_predicate.getValue()), _numDocs));
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
    StringBuilder stringBuilder = new StringBuilder(getExplainPlanName()).append("(indexLookUp:json_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
