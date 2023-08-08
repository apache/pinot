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
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct only queries with single dictionary-encoded column.
 */
public class DictionaryBasedSingleColumnDistinctOnlyExecutor extends BaseDictionaryBasedSingleColumnDistinctExecutor {

  public DictionaryBasedSingleColumnDistinctOnlyExecutor(ExpressionContext expression, Dictionary dictionary,
      DataType dataType, int limit) {
    super(expression, dictionary, dataType, limit, false);
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expression);
    int numDocs = valueBlock.getNumDocs();
    if (blockValueSet.isSingleValue()) {
      int[] dictIds = blockValueSet.getDictionaryIdsSV();
      for (int i = 0; i < numDocs; i++) {
        _dictIdSet.add(dictIds[i]);
        if (_dictIdSet.size() >= _limit) {
          return true;
        }
      }
    } else {
      int[][] dictIds = blockValueSet.getDictionaryIdsMV();
      for (int i = 0; i < numDocs; i++) {
        for (int dictId : dictIds[i]) {
          _dictIdSet.add(dictId);
          if (_dictIdSet.size() >= _limit) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
