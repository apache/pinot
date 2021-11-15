package org.apache.pinot.core.operator.blocks;

import java.util.List;

import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;


public class CombinedTransformBlock extends TransformBlock {
  protected List<TransformBlock> _transformBlockList;
  protected TransformBlock _nonFilteredAggBlock;

  public CombinedTransformBlock(List<TransformBlock> transformBlockList,
      TransformBlock nonFilteredAggBlock) {
    super(nonFilteredAggBlock == null ? null : nonFilteredAggBlock._projectionBlock,
        nonFilteredAggBlock == null ? null : nonFilteredAggBlock._transformFunctionMap);

    _transformBlockList = transformBlockList;
    _nonFilteredAggBlock = nonFilteredAggBlock;
  }

  public int getNumDocs() {
    int numDocs = 0;

    for (TransformBlock transformBlock : _transformBlockList) {
      if (transformBlock != null) {
        numDocs = numDocs + transformBlock._projectionBlock.getNumDocs();
      }
    }

    if (_nonFilteredAggBlock != null) {
      numDocs = numDocs + _nonFilteredAggBlock.getNumDocs();
    }

    return numDocs;
  }

  public List<TransformBlock> getTransformBlockList() {
    return _transformBlockList;
  }

  public TransformBlock getNonFilteredAggBlock() {
    return _nonFilteredAggBlock;
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
