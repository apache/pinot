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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.docvalsets.TransformBlockValSet;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * This class implements the Addition transformation.
 */
@NotThreadSafe
public class AdditionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "add";
  private double[] _sum; // Result of addition transform.

  /**
   * {@inheritDoc}
   *
   * This method applies the addition transformation.
   * Assumes that input objects are double arrays.
   *
   * @param length Length of doc ids to process
   * @param inputs Array of input values
   * @return BlockValSet containing transformed values.
   */
  @Override
  public double[] transform(int length, @Nonnull BlockValSet... inputs) {
    if (_sum == null || _sum.length < length) {
      _sum = new double[Math.max(length, DocIdSetPlanNode.MAX_DOC_PER_CALL)];
    }

    for (BlockValSet input : inputs) {
      double[] values = input.getDoubleValuesSV();
      for (int j = 0; j < length; j++) {
        _sum[j] += values[j];
      }
    }
    return _sum;
  }

  @Override
  public FieldSpec.DataType getOutputType() {
    return FieldSpec.DataType.DOUBLE;
  }

  /**
   * {@inheritDoc}
   * @return {@link #TRANSFORM_NAME}
   */
  @Override
  public String getName() {
    return TRANSFORM_NAME;
  }
}
