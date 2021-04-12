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
package org.apache.pinot.core.query.distinct.dictionary;

import java.util.List;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * {@link DistinctExecutor} for distinct only queries with multiple dictionary-encoded columns.
 */
public class DictionaryBasedMultiColumnDistinctOnlyExecutor extends BaseDictionaryBasedMultiColumnDistinctExecutor {

  public DictionaryBasedMultiColumnDistinctOnlyExecutor(List<ExpressionContext> expressions,
      List<Dictionary> dictionaries, int limit) {
    super(expressions, dictionaries, limit);
  }

  @Override
  public boolean process(TransformBlock transformBlock) {
    int numDocs = transformBlock.getNumDocs();
    int numExpressions = _expressions.size();
    int[][] dictIdsArray = new int[numDocs][numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_expressions.get(i));
      int[] dictIdsForExpression = blockValueSet.getDictionaryIdsSV();
      for (int j = 0; j < numDocs; j++) {
        dictIdsArray[j][i] = dictIdsForExpression[j];
      }
    }
    for (int i = 0; i < numDocs; i++) {
      _dictIdsSet.add(new DictIds(dictIdsArray[i]));
      if (_dictIdsSet.size() >= _limit) {
        return true;
      }
    }
    return false;
  }
}
