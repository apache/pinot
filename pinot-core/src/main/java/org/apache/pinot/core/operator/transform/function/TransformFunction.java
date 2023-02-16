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
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


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
  void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap);

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
   * Returns the dictionary for the transform result if the result is dictionary-encoded, or {@code null} if not.
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
  int[] transformToDictIdsSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, int[]> transformToDictIdsSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued dictionary Ids.
   *
   * @param projectionBlock Projection block
   * @return Transformation result
   */
  int[][] transformToDictIdsMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, int[][]> transformToDictIdsMVWithNull(ProjectionBlock projectionBlock);

  /**
   * SINGLE-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to single-valued int values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  int[] transformToIntValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, int[]> transformToIntValuesSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued long values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  long[] transformToLongValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, long[]> transformToLongValuesSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued float values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  float[] transformToFloatValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, float[]> transformToFloatValuesSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued double values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, double[]> transformToDoubleValuesSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued BigDecimal values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, BigDecimal[]> transformToBigDecimalValuesSVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to single-valued string values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  String[] transformToStringValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, String[]> transformToStringValuesSVWithNull(ProjectionBlock projectionBlock);


  /**
   * Transforms the data from the given projection block to single-valued bytes values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, byte[][]> transformToBytesValuesSVWithNull(ProjectionBlock projectionBlock);


  /**
   * MULTI-VALUED APIs
   */

  /**
   * Transforms the data from the given projection block to multi-valued int values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  int[][] transformToIntValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, int[][]> transformToIntValuesMVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued long values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  long[][] transformToLongValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, long[][]> transformToLongValuesMVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued float values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, float[][]> transformToFloatValuesMVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued double values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, double[][]> transformToDoubleValuesMVWithNull(ProjectionBlock projectionBlock);

  /**
   * Transforms the data from the given projection block to multi-valued string values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  String[][] transformToStringValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, String[][]> transformToStringValuesMVWithNull(ProjectionBlock projectionBlock);


  /**
   * Transforms the data from the given projection block to multi-valued bytes values.
   *
   * @param projectionBlock Projection result
   * @return Transformation result
   */
  byte[][][] transformToBytesValuesMV(ProjectionBlock projectionBlock);

  Pair<RoaringBitmap, byte[][][]> transformToBytesValuesMVWithNull(ProjectionBlock projectionBlock);


  RoaringBitmap getNullBitmap(ProjectionBlock projectionBlock);
}
