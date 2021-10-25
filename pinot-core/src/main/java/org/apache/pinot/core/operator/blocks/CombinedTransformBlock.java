package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.core.common.*;

import java.util.Iterator;
import java.util.Map;

public class CombinedTransformBlock<T> implements WrapperBlock<T> {
  protected Map<T, TransformBlock> _transformBlockMap;

  public CombinedTransformBlock(Map<T, TransformBlock> transformBlockMap) {
    assert transformBlockMap != null;

    _transformBlockMap = transformBlockMap;
  }

  public int getNumDocs() {
    int numDocs = 0;
    Iterator<Map.Entry<T, TransformBlock>> iterator = _transformBlockMap.entrySet().iterator();

    while (iterator.hasNext()) {
      numDocs = numDocs + iterator.next().getValue()._projectionBlock.getNumDocs();
    }

    return numDocs;
  }

  @Override
  public TransformBlock getTransformBlock(T key) {
    return _transformBlockMap.get(key);
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
