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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.RoaringBitmap;


/**
 * <code>DistinctFromTransformFunction</code> abstracts the transform needed for IsDistinctFrom and IsNotDistinctFrom.
 * Null value is considered as distinct from non-null value.
 * When both values are not null, this function calls equal transform function to determined whether two values are
 * distinct.
 * This function only supports two arguments which are both column names.
 */
public class DistinctFromTransformFunction extends BinaryOperatorTransformFunction {
  // If _distinctType is 1, act as IsDistinctFrom transformation, otherwise act as IsNotDistinctFrom.
  // This value should only be 0 or 1.
  private int _distinctType;

  /**
   * Returns a bit map of corresponding column.
   * Returns null by default if null option is disabled.
   */
  private static @Nullable RoaringBitmap getNullBitMap(ProjectionBlock projectionBlock,
      TransformFunction transformFunction) {
    String columnName = ((IdentifierTransformFunction) transformFunction).getColumnName();
    RoaringBitmap nullBitmap = projectionBlock.getBlockValueSet(columnName).getNullBitmap();
    return nullBitmap;
  }

  /**
   * Fill in results based on functions
   * @param isDistinctNull return true if one field is null and the other field is not null
   * @param isBothNull return true if both fields are null
   * @param length length of the result
   */
  private void fillInDistinctNullValue(Function<Integer, Boolean> isDistinctNull, Function<Integer, Boolean> isBothNull,
      int length) {
    for (int i = 0; i < length; i++) {
      if (isDistinctNull.apply(i)) {
        _results[i] = _distinctType;
      } else if (isBothNull.apply(i)) {
        _results[i] = _distinctType ^ 1;
      }
    }
  }

  /**
   * @param distinctType is set to true for IsDistinctFrom, otherwise it is for IsNotDistinctFrom.
   */
  protected DistinctFromTransformFunction(boolean distinctType) {
    super(distinctType ? TransformFunctionType.NOT_EQUALS : TransformFunctionType.EQUALS);
    _distinctType = distinctType ? 1 : 0;
  }

  @Override
  public String getName() {
    if (_distinctType == 1) {
      return TransformFunctionType.IS_DISTINCT_FROM.getName();
    }
    return TransformFunctionType.IS_NOT_DISTINCT_FROM.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    super.init(arguments, dataSourceMap);
    if (!(_leftTransformFunction instanceof IdentifierTransformFunction)
        || !(_rightTransformFunction instanceof IdentifierTransformFunction)) {
      throw new IllegalArgumentException("Only column names are supported in DistinctFrom transformation.");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    _results = super.transformToIntValuesSV(projectionBlock);
    RoaringBitmap leftNull = getNullBitMap(projectionBlock, _leftTransformFunction);
    RoaringBitmap rightNull = getNullBitMap(projectionBlock, _rightTransformFunction);
    if (leftNull == null && rightNull == null) {
      return _results;
    }
    if (leftNull == null) {
      fillInDistinctNullValue(rightNull::contains, (Integer i) -> false, length);
      return _results;
    }
    if (rightNull == null) {
      fillInDistinctNullValue(leftNull::contains, (Integer i) -> false, length);
      return _results;
    }
    RoaringBitmap xorNull = RoaringBitmap.xor(leftNull, rightNull);
    fillInDistinctNullValue(xorNull::contains, rightNull::contains, length);
    return _results;
  }
}
