/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Transform Block holds blocks of transformed columns.
 * <p>In absence of transforms, it servers as a pass-through to projection block.
 */
public class TransformBlock implements Block {
  private final ProjectionBlock _projectionBlock;
  private final Map<TransformExpressionTree, BlockValSet> _blockValSetMap;

  public TransformBlock(@Nonnull ProjectionBlock projectionBlock,
      @Nonnull Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    _projectionBlock = projectionBlock;
    _blockValSetMap = blockValSetMap;
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
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  public BlockValSet getBlockValueSet(String column) {
    return _blockValSetMap.get(TransformExpressionTree.compileToExpressionTree(column));
  }

  // TODO: metadata should be fetched from Operator instead of block
  // TODO: Need to support dictionary for transformed block
  public BlockMetadata getBlockMetadata(String column) {
    Block block = _projectionBlock.getBlock(column);
    if (block != null) {
      return block.getMetadata();
    } else {
      BlockValSet blockValueSet = getBlockValueSet(column);
      return new BlockMetadataImpl(blockValueSet.getNumDocs(), true, 0, blockValueSet.getValueType(), null);
    }
  }

  public int getNumDocs() {
    return _projectionBlock.getNumDocs();
  }
}
