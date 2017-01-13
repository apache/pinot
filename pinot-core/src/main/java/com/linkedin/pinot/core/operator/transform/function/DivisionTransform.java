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
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.docvalsets.TransformBlockValSet;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import javax.annotation.Nonnull;


/**
 * This class implements the Division transform function.
 */
public class DivisionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "div";
  private double[] _result;

  /**
   * {@inheritDoc}
   *
   * Assumes that there are two input objects, each of which
   * is a double array.
   *
   * @param length Length of doc ids to process
   * @param input Array of input values
   * @return BlockValSet containing transformed values.
   */
  @Override
  public double[] transform(int length, @Nonnull BlockValSet... input) {
    Preconditions.checkArgument((input.length == 2), "Division transform expects exactly two arguments");

    if (_result == null || _result.length < length) {
      _result = new double[Math.max(length, DocIdSetPlanNode.MAX_DOC_PER_CALL)];
    }

    double[] numerators = input[0].getDoubleValuesSV();
    double[] denominators = input[1].getDoubleValuesSV();

    for (int i = 0; i < length; i++) {
      _result[i] = numerators[i] / denominators[i];
    }
    return _result;
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
