/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Interface for transform functions.
 */
public interface TransformFunction {

  /**
   * Returns the name of the transform function.
   * <p>This name should be unique among all transform functions.
   *
   * @return Name of the transform function
   */
  String getName();

  /**
   * Initializes the transform function.
   *
   * @param arguments Arguments for the transform function
   * @param dataSourceMap Map from column to data source
   */
  void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap);

  /**
   * Returns the metadata for the result of the transform function.
   *
   * @return Transform result metadata
   */
  TransformResultMetadata getResultMetadata();

  /**
   * DICTIONARY BASED APIs
   */

  /**
   * Returns the dictionary that the dictionary Ids based on.
   *
   * @return Dictionary
   */
  Dictionary getDictionary();

  /**
   * Transforms the data from the given projection block to single-valued dictionary Ids.
   *
   * @param projectionBlock Projection block
   * @return Transformation result
   */
  int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued dictionary Ids.
   *
   * @param projectionBlock Projection block
   * @return Transformation result
   */
  int[][] transformToDictIdsMV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * SINGLE-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to single-valued int values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued long values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued float values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued double values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued string values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * MULTI-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to multi-valued int values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued long values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued float values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued double values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued string values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock);
}
