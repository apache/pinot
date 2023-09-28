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

import java.math.BigDecimal;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.evaluator.TransformEvaluator;
import org.apache.pinot.spi.data.readers.Vector;


public interface PushDownTransformFunction {

  /**
   * SINGLE-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to single-valued int values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToIntValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, int[] buffer);

  /**
   * Transforms the data from the given projection block to single-valued long values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToLongValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, long[] buffer);

  /**
   * Transforms the data from the given projection block to single-valued float values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToFloatValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, float[] buffer);

  /**
   * Transforms the data from the given projection block to single-valued double values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToDoubleValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, double[] buffer);

  /**
   * Transforms the data from the given projection block to single-valued BigDecimal values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToBigDecimalValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      BigDecimal[] buffer);

  /**
   * Transforms the data from the given projection block to single-valued string values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToStringValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, String[] buffer);

  /**
   * Transforms the data from the given projection block to Vector values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToVectorValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      Vector[] buffer);

  /**
   * MULTI-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to multi-valued int values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToIntValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, int[][] buffer);

  /**
   * Transforms the data from the given projection block to multi-valued long values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToLongValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, long[][] buffer);

  /**
   * Transforms the data from the given projection block to multi-valued float values.
   *
   * @param projectionBlock Projection result
   * @param jsonPath transform
   * @param buffer values to fill
   */
  void transformToFloatValuesMV(ProjectionBlock projectionBlock, TransformEvaluator jsonPath, float[][] buffer);

  /**
   * Transforms the data from the given projection block to multi-valued double values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToDoubleValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, double[][] buffer);

  /**
   * Transforms the data from the given projection block to multi-valued string values.
   *
   * @param projectionBlock Projection result
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  void transformToStringValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, String[][] buffer);
}
