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

import com.linkedin.pinot.common.request.transform.TransformFunction;
import com.linkedin.pinot.common.request.transform.result.TransformResult;
import com.linkedin.pinot.core.operator.transform.result.DoubleArrayTransformResult;
import javax.annotation.Nonnull;


/**
 * This class implements the Addition transformation.
 */
public class AdditionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "add";

  /**
   * {@inheritDoc}
   *
   * This method applies the addition transformation.
   * Assumes that input objects are double arrays.
   *
   * @param length Length of doc ids to process
   * @param inputs Array of input values
   * @return TransformResult containing results values
   */
  @Override
  public TransformResult transform(int length, @Nonnull Object... inputs) {
    double[] sum = new double[((double[]) inputs[0]).length];

    for (Object input : inputs) {
      double[] values = (double[]) input;
      for (int j = 0; j < length; j++) {
        sum[j] += values[j];
      }
    }
    return new DoubleArrayTransformResult(sum);
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
