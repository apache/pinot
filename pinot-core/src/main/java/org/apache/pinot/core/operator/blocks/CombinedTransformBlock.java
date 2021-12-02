package org.apache.pinot.core.operator.blocks;

import java.util.List;

import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;


public class CombinedTransformBlock extends TransformBlock {
  protected List<TransformBlock> _transformBlockList;

  public CombinedTransformBlock(List<TransformBlock> transformBlockList) {

    super(transformBlockList.get(0)._projectionBlock,
        transformBlockList.get(0)._transformFunctionMap);

    _transformBlockList = transformBlockList;
  }

  public int getNumDocs() {
    int numDocs = 0;

    for (TransformBlock transformBlock : _transformBlockList) {
      numDocs = numDocs + transformBlock._projectionBlock.getNumDocs();
    }

    return numDocs;
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
