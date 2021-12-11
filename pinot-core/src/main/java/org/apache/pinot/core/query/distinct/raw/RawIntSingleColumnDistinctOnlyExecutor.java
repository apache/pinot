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
package org.apache.pinot.core.query.distinct.raw;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct only queries with single raw INT column.
 */
public class RawIntSingleColumnDistinctOnlyExecutor extends BaseRawIntSingleColumnDistinctExecutor {

  public RawIntSingleColumnDistinctOnlyExecutor(ExpressionContext expression, DataType dataType, int limit) {
    super(expression, dataType, limit);
  }

  @Override
  public boolean process(TransformBlock transformBlock) {
    BlockValSet blockValueSet = transformBlock.getBlockValueSet(_expression);
    int[] values = blockValueSet.getIntValuesSV();
    int numDocs = transformBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      _valueSet.add(values[i]);
      if (_valueSet.size() >= _limit) {
        return true;
      }
    }
    return false;
  }
}
