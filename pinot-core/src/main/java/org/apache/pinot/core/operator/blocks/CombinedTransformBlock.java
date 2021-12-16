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
package org.apache.pinot.core.operator.blocks;

import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;

/**
 * Represents a combination of multiple TransformBlock instances
 */
public class CombinedTransformBlock extends TransformBlock {
  protected Map<ExpressionContext, TransformBlock> _transformBlockMap;
  protected ExpressionContext _mainPredicateExpressionContext;

  public CombinedTransformBlock(Map<ExpressionContext, TransformBlock> transformBlockMap,
      ExpressionContext mainPredicateExpressionContext) {
    super(transformBlockMap.get(mainPredicateExpressionContext) == null ? null
            : transformBlockMap.get(mainPredicateExpressionContext)._projectionBlock,
        transformBlockMap.get(mainPredicateExpressionContext) == null ? null
            : transformBlockMap.get(mainPredicateExpressionContext)._transformFunctionMap);

    _transformBlockMap = transformBlockMap;
    _mainPredicateExpressionContext = mainPredicateExpressionContext;
  }

  public int getNumDocs() {
    int numDocs = 0;

    Iterator<Map.Entry<ExpressionContext, TransformBlock>> iterator = _transformBlockMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<ExpressionContext, TransformBlock> entry = iterator.next();
      TransformBlock transformBlock = entry.getValue();

      if (transformBlock != null) {
        numDocs = numDocs + transformBlock._projectionBlock.getNumDocs();
      }
    }

    return numDocs;
  }

  public Map<ExpressionContext, TransformBlock> getTransformBlockMap() {
    return _transformBlockMap;
  }

  public TransformBlock getNonFilteredAggBlock() {
    return _transformBlockMap.get(_mainPredicateExpressionContext);
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }
}
