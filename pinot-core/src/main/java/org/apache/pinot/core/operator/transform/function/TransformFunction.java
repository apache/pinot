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
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
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
   * @param arguments        Arguments for the transform function
   * @param columnContextMap Map from column name to context
   */
  void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap);

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
   */
  @Nullable
  Dictionary getDictionary();

  /**
   * Transforms the data from the given value block to single-valued dictionary ids.
   */
  int[] transformToDictIdsSV(ValueBlock valueBlock);

  default Pair<int[], RoaringBitmap> transformToDictIdsSVWithNull(ValueBlock block) {
    return ImmutablePair.of(transformToDictIdsSV(block), getNullBitmap(block));
  }

  /**
   * Transforms the data from the given value block to multi-valued dictionary ids.
   */
  int[][] transformToDictIdsMV(ValueBlock valueBlock);

  default Pair<int[][], RoaringBitmap> transformToDictIdsMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToDictIdsMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * SINGLE-VALUED APIs
   */

  /**
   * Transforms the data from the given value block to single-valued int values.
   */
  int[] transformToIntValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued int values with null bit vector.
   */
  default Pair<int[], RoaringBitmap> transformToIntValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToIntValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued long values.
   */
  long[] transformToLongValuesSV(ValueBlock valueBlock);


  /**
   * Transforms the data from the given value block to single-valued long values with null bit vector.
   */
  default Pair<long[], RoaringBitmap> transformToLongValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToLongValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued float values.
   */
  float[] transformToFloatValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued float values with null bit vector.
   */
  default Pair<float[], RoaringBitmap> transformToFloatValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToFloatValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued double values.
   */
  double[] transformToDoubleValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued double values with null bit vector.
   */
  default Pair<double[], RoaringBitmap> transformToDoubleValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToDoubleValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued BigDecimal values.
   */
  BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock);


  /**
   * Transforms the data from the given projection block to single-valued BigDecimal values and null bit vector.
   */
  default Pair<BigDecimal[], RoaringBitmap> transformToBigDecimalValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToBigDecimalValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued string values.
   */
  String[] transformToStringValuesSV(ValueBlock valueBlock);


  /**
   * Transforms the data from the given projection block to single-valued string values and null bit vector.
   */
  default Pair<String[], RoaringBitmap> transformToStringValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToStringValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to single-valued bytes values.
   */
  byte[][] transformToBytesValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given projection block to single-valued bytes values and null bit vector.
   */
  default Pair<byte[][], RoaringBitmap> transformToBytesValuesSVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToBytesValuesSV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * MULTI-VALUED APIs
   */

  /**
   * Transforms the data from the given value block to multi-valued int values.
   */
  int[][] transformToIntValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued double values and null bit vector.
   */
  default Pair<int[][], RoaringBitmap> transformToIntValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToIntValuesMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to multi-valued long values.
   */
  long[][] transformToLongValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued double values and null bit vector.
   */
  default Pair<long[][], RoaringBitmap> transformToLongValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToLongValuesMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to multi-valued float values.
   */
  float[][] transformToFloatValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued double values and null bit vector.
   */
  default Pair<float[][], RoaringBitmap> transformToFloatValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToFloatValuesMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to multi-valued double values.
   */
  double[][] transformToDoubleValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given projection block to multi-valued double values and null bit vector.
   */
  default Pair<double[][], RoaringBitmap> transformToDoubleValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToDoubleValuesMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Transforms the data from the given value block to multi-valued string values.
   */
  String[][] transformToStringValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given projection block to multi-valued string values and null bit vector.
   */
  default Pair<String[][], RoaringBitmap> transformToStringValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToStringValuesMV(valueBlock), getNullBitmap(valueBlock));
  }


  /**
   * Transforms the data from the given value block to multi-valued bytes values.
   */
  byte[][][] transformToBytesValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given projection block to multi-valued bytes values and null bit vector.
   */
  default Pair<byte[][][], RoaringBitmap> transformToBytesValuesMVWithNull(ValueBlock valueBlock) {
    return ImmutablePair.of(transformToBytesValuesMV(valueBlock), getNullBitmap(valueBlock));
  }

  /**
   * Gets the null rows for transformation result. Should be called when only null information is needed for
   * transformation.
   *
   * @return Null bit vector that indicates null rows for transformation result
   * If returns null, it means no record is null.
   */
  @Nullable
  RoaringBitmap getNullBitmap(ValueBlock block);
}
