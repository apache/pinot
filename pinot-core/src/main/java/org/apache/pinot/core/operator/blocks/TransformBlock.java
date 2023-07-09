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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.docvalsets.TransformBlockValSet;
import org.apache.pinot.core.operator.transform.function.TransformFunction;


/**
 * The {@code TransformBlock} contains values of the transformed columns.
 */
public class TransformBlock implements ValueBlock {
  protected final ValueBlock _sourceBlock;
  protected final Map<ExpressionContext, TransformFunction> _transformFunctionMap;

  public TransformBlock(ValueBlock sourceBlock, Map<ExpressionContext, TransformFunction> transformFunctionMap) {
    _sourceBlock = sourceBlock;
    _transformFunctionMap = transformFunctionMap;
  }

  @Override
  public int getNumDocs() {
    return _sourceBlock.getNumDocs();
  }

  @Nullable
  @Override
  public int[] getDocIds() {
    return _sourceBlock.getDocIds();
  }

  @Override
  public BlockValSet getBlockValueSet(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      return _sourceBlock.getBlockValueSet(expression);
    } else {
      return new TransformBlockValSet(_sourceBlock, _transformFunctionMap.get(expression));
    }
  }

  @Override
  public BlockValSet getBlockValueSet(String column) {
    return _sourceBlock.getBlockValueSet(column);
  }
}
