package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.roaringbitmap.RoaringBitmap;


public abstract class BooleanAssertionTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() == 1, "Exact 1 argument is required for " + getName());
    _transformFunction = arguments.get(0);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    Arrays.fill(_intValuesSV, 0);
    int[] intValuesSV = _transformFunction.transformToIntValuesSV(valueBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(valueBlock);
    }
    for (int docId = 0; docId < length; docId++) {
      if (isNull(nullBitmap, docId)) {
        if (returnsTrueWhenValueIsNull()) {
          _intValuesSV[docId] = 1;
        }
      } else if (isTrue(intValuesSV[docId])) {
        if (returnsTrueWhenValueIsTrue()) {
          _intValuesSV[docId] = 1;
        }
      } else if (returnsTrueWhenValueIsFalse()) {
        _intValuesSV[docId] = 1;
      }
    }
    return _intValuesSV;
  }

  private boolean isNull(RoaringBitmap nullBitmap, int i) {
    return nullBitmap != null && nullBitmap.contains(i);
  }

  private boolean isTrue(int i) {
    return i != 0;
  }

  abstract boolean returnsTrueWhenValueIsTrue();

  abstract boolean returnsTrueWhenValueIsFalse();

  abstract boolean returnsTrueWhenValueIsNull();

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return null;
  }
}
