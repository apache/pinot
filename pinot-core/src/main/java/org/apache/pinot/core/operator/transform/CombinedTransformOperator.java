package org.apache.pinot.core.operator.transform;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;

public class CombinedTransformOperator<T> extends BaseOperator<CombinedTransformBlock<T>> {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  protected final Map<T, TransformOperator> _transformOperatorMap;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(Map<T, TransformOperator> transformOperatorMap) {
    _transformOperatorMap = transformOperatorMap;
  }

  @Override
  protected CombinedTransformBlock<T> getNextBlock() {
    Map<T, TransformBlock> transformBlockMap = new HashMap<>();
    Iterator<Map.Entry<T, TransformOperator>> iterator = _transformOperatorMap.entrySet().iterator();

    // Get next block from all underlying transform operators
    while (iterator.hasNext()) {
      Map.Entry<T, TransformOperator> entry = iterator.next();

      transformBlockMap.put(entry.getKey(), entry.getValue().nextBlock());
    }

    return new CombinedTransformBlock<>(transformBlockMap);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
