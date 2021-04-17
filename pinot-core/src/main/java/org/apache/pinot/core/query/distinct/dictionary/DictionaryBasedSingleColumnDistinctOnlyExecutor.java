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

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * {@link DistinctExecutor} for distinct only queries with single dictionary-encoded column.
 */
public class DictionaryBasedSingleColumnDistinctOnlyExecutor extends BaseDictionaryBasedSingleColumnDistinctExecutor {

  public DictionaryBasedSingleColumnDistinctOnlyExecutor(ExpressionContext expression, Dictionary dictionary,
      int limit) {
    super(expression, dictionary, limit);
  }

  @Override
  public boolean process(TransformBlock transformBlock) {
    BlockValSet blockValueSet = transformBlock.getBlockValueSet(_expression);
    int[] dictIds = blockValueSet.getDictionaryIdsSV();
    int numDocs = transformBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      _dictIdSet.add(dictIds[i]);
      if (_dictIdSet.size() >= _limit) {
        return true;
      }
    }
    return false;
  }
}
