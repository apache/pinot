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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.docvalsets.TransformBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Transform Block holds blocks of transformed columns.
 * <p>In absence of transforms, it servers as a pass-through to projection block.
 */
public class TransformBlock implements Block {
  private final ProjectionBlock _projectionBlock;
  private final Map<TransformExpressionTree, TransformFunction> _transformFunctionMap;

  public TransformBlock(@Nonnull ProjectionBlock projectionBlock,
      @Nonnull Map<TransformExpressionTree, TransformFunction> transformFunctionMap) {
    _projectionBlock = projectionBlock;
    _transformFunctionMap = transformFunctionMap;
  }

  public int getNumDocs() {
    return _projectionBlock.getNumDocs();
  }

  public BlockValSet getBlockValueSet(TransformExpressionTree expression) {
    if (expression.isColumn()) {
      return _projectionBlock.getBlockValueSet(expression.getValue());
    } else {
      return new TransformBlockValSet(_projectionBlock, _transformFunctionMap.get(expression));
    }
  }

  public BlockValSet getBlockValueSet(String column) {
    return _projectionBlock.getBlockValueSet(column);
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
