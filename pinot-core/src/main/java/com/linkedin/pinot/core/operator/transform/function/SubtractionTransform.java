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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformFunction;
import com.linkedin.pinot.common.request.transform.result.TransformResult;
import com.linkedin.pinot.core.operator.transform.result.DoubleArrayTransformResult;
import javax.annotation.Nonnull;


/**
 * This class implements the Subtraction transform function.
 */
public class SubtractionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "sub";

  /**
   * {@inheritDoc}
   * Assumes that there are two in objects, each of which is a double array.
   *
   * @param length Length of doc ids to process
   * @param input Array of input values
   * @return TransformResult containing result values
   */
  @Override
  public TransformResult transform(int length, @Nonnull Object... input) {
    Preconditions.checkArgument((input.length == 2), "Subtraction transform expects exactly two arguments");

    double[] firsts = (double[]) input[0];
    double[] seconds = (double[]) input[1];

    double[] result = new double[length];
    for (int i = 0; i < length; i++) {
      result[i] = firsts[i] - seconds[i];
    }
    return new DoubleArrayTransformResult(result);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link #TRANSFORM_NAME}
   */
  @Override
  public String getName() {
    return TRANSFORM_NAME;
  }
}
