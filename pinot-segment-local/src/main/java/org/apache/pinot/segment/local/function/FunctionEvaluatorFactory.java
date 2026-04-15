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
package org.apache.pinot.segment.local.function;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.function.FunctionEvaluator;


/**
 * Deprecated factory shim that preserves the historical entry points under
 * {@code org.apache.pinot.segment.local.function}. Delegates entirely to
 * {@link org.apache.pinot.common.evaluator.FunctionEvaluatorFactory}.
 *
 * <p>This class is stateless and thread-safe.
 *
 * <p>TODO: Delete this shim after Pinot 1.6.0 is released.
 *
 * @deprecated Use {@link org.apache.pinot.common.evaluator.FunctionEvaluatorFactory} instead.
 */
@Deprecated
public class FunctionEvaluatorFactory {
  private FunctionEvaluatorFactory() {
  }

  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    return org.apache.pinot.common.evaluator.FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
  }

  public static FunctionEvaluator getExpressionEvaluator(String transformExpression) {
    return org.apache.pinot.common.evaluator.FunctionEvaluatorFactory.getExpressionEvaluator(transformExpression);
  }

  public static boolean isGroovyExpression(String transformExpression) {
    return org.apache.pinot.common.evaluator.FunctionEvaluatorFactory.isGroovyExpression(transformExpression);
  }
}
