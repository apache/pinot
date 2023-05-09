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

import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


/**
 * <code>DistinctFromTransformFunction</code> abstracts the transform needed for IsDistinctFrom and IsNotDistinctFrom.
 * Null value is considered as distinct from non-null value.
 * When both values are not null, this function calls equal transform function to determined whether two values are
 * distinct.
 * This function only supports two arguments which are both column names.
 */
public class DistinctFromTransformFunction extends BinaryOperatorTransformFunction {
  // Result value to save when two values are distinct.
  // 1 for isDistinct, 0 for isNotDistinct
  private final int _distinctResult;
  // Result value to save when two values are not distinct.
  // 0 for isDistinct, 1 for isNotDistinct
  private final int _notDistinctResult;

  /**
   * Returns true when bitmap is null (null option is disabled) or bitmap is empty.
   */
  private static boolean isEmpty(RoaringBitmap bitmap) {
    return bitmap == null || bitmap.isEmpty();
  }

  /**
   * @param distinct is set to true for IsDistinctFrom, otherwise it is for IsNotDistinctFrom.
   */
  protected DistinctFromTransformFunction(boolean distinct) {
    super(distinct ? TransformFunctionType.NOT_EQUALS : TransformFunctionType.EQUALS);
    _distinctResult = distinct ? 1 : 0;
    _notDistinctResult = distinct ? 0 : 1;
  }

  @Override
  public String getName() {
    if (_distinctResult == 1) {
      return TransformFunctionType.IS_DISTINCT_FROM.getName();
    }
    return TransformFunctionType.IS_NOT_DISTINCT_FROM.getName();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    _intValuesSV = super.transformToIntValuesSV(valueBlock);
    RoaringBitmap leftNull = _leftTransformFunction.getNullBitmap(valueBlock);
    RoaringBitmap rightNull = _rightTransformFunction.getNullBitmap(valueBlock);
    // Both sides are not null.
    if (isEmpty(leftNull) && isEmpty(rightNull)) {
      return _intValuesSV;
    }
    // Left side is not null.
    if (isEmpty(leftNull)) {
      // Mark right null rows as distinct.
      rightNull.forEach((IntConsumer) i -> _intValuesSV[i] = _distinctResult);
      return _intValuesSV;
    }
    // Right side is not null.
    if (isEmpty(rightNull)) {
      // Mark left null rows as distinct.
      leftNull.forEach((IntConsumer) i -> _intValuesSV[i] = _distinctResult);
      return _intValuesSV;
    }
    RoaringBitmap xorNull = RoaringBitmap.xor(leftNull, rightNull);
    // For rows that with one null and one not null, mark them as distinct
    xorNull.forEach((IntConsumer) i -> _intValuesSV[i] = _distinctResult);
    RoaringBitmap andNull = RoaringBitmap.and(leftNull, rightNull);
    // For rows that are both null, mark them as not distinct.
    andNull.forEach((IntConsumer) i -> _intValuesSV[i] = _notDistinctResult);
    return _intValuesSV;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return null;
  }
}
