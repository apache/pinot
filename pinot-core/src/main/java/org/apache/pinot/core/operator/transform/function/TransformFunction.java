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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
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
   * Initializes the transform function.
   *
   * @param arguments           Arguments for the transform function
   * @param columnContextMap    Map from column name to context
   * @param nullHandlingEnabled Whether this transform function handles {@code null}
   */
  default void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    init(arguments, columnContextMap);
  }

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

  /**
   * Transforms the data from the given value block to multi-valued dictionary ids.
   */
  int[][] transformToDictIdsMV(ValueBlock valueBlock);

  /**
   * SINGLE-VALUED APIs
   */

  /**
   * Transforms the data from the given value block to single-valued int values.
   */
  int[] transformToIntValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued long values.
   */
  long[] transformToLongValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued float values.
   */
  float[] transformToFloatValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued double values.
   */
  double[] transformToDoubleValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued BigDecimal values.
   */
  BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued string values.
   */
  String[] transformToStringValuesSV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to single-valued bytes values.
   */
  byte[][] transformToBytesValuesSV(ValueBlock valueBlock);

  /**
   * MULTI-VALUED APIs
   */

  /**
   * Transforms the data from the given value block to multi-valued int values.
   */
  int[][] transformToIntValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued long values.
   */
  long[][] transformToLongValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued float values.
   */
  float[][] transformToFloatValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued double values.
   */
  double[][] transformToDoubleValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued string values.
   */
  String[][] transformToStringValuesMV(ValueBlock valueBlock);

  /**
   * Transforms the data from the given value block to multi-valued bytes values.
   */
  byte[][][] transformToBytesValuesMV(ValueBlock valueBlock);

  /**
   * Gets the null rows for transformation result. Should be called when only null information is needed for
   * transformation.
   *
   * @return Null bit vector that indicates null rows for transformation result
   * If returns null, it means no record is null.
   */
  @Nullable
  RoaringBitmap getNullBitmap(ValueBlock block);

  /**
   * Validates transform function configuration during table creation.
   */
  default void validateIngestionConfig(String transformFunctionExpression, 
      org.apache.pinot.spi.data.Schema schema) {
    // Default: no validation
  }

  /**
   * Returns whether this function supports ingestion-time transformation.
   */
  default boolean supportsIngestionTransform() {
    return true;
  }

  /**
   * Infers output data type based on input arguments.
   */
  default org.apache.pinot.spi.data.FieldSpec.DataType inferOutputDataType(
      List<String> inputArguments, org.apache.pinot.spi.data.Schema schema) {
    return org.apache.pinot.spi.data.FieldSpec.DataType.STRING;
  }

  /**
   * Returns expected input data types for validation.
   */
  default org.apache.pinot.spi.data.FieldSpec.DataType[] getExpectedInputDataTypes() {
    return new org.apache.pinot.spi.data.FieldSpec.DataType[0];
  }

  /**
   * Returns minimum number of arguments required.
   */
  default int getMinArgumentCount() {
    return 0;
  }

  /**
   * Returns maximum number of arguments allowed (-1 for unlimited).
   */
  default int getMaxArgumentCount() {
    return -1;
  }

  /**
   * Validates input data type compatibility.
   */
  default boolean isInputDataTypeSupported(org.apache.pinot.spi.data.FieldSpec.DataType inputType, 
      int argumentIndex) {
    return true;
  }
}
