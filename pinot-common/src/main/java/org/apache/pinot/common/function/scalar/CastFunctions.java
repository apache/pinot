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
package org.apache.pinot.common.function.scalar;

import java.math.BigDecimal;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * CAST functions based on input type and output type info.
 *
 * the input data type must be one of: {@link FieldSpec.DataType}.
 */
public class CastFunctions {
  private CastFunctions() {
  }

  @ScalarFunction
  public static int castInt(Object inputValue, String inputType) {
    FieldSpec.DataType inputDataType = FieldSpec.DataType.valueOf(inputType);
    switch (inputDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        return ((Number) inputValue).intValue();
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("Unsupported cast from type: " + inputType + " to INTEGER.");
    }
  }

  @ScalarFunction
  public static long castLong(Object inputValue, String inputType) {
    FieldSpec.DataType inputDataType = FieldSpec.DataType.valueOf(inputType);
    switch (inputDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        return ((Number) inputValue).longValue();
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("Unsupported cast from type: " + inputType + " to INTEGER.");
    }
  }

  @ScalarFunction
  public static float castFloat(Object inputValue, String inputType) {
    FieldSpec.DataType inputDataType = FieldSpec.DataType.valueOf(inputType);
    switch (inputDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        return ((Number) inputValue).floatValue();
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("Unsupported cast from type: " + inputType + " to INTEGER.");
    }
  }

  @ScalarFunction
  public static double castDouble(Object inputValue, String inputType) {
    FieldSpec.DataType inputDataType = FieldSpec.DataType.valueOf(inputType);
    switch (inputDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        return ((Number) inputValue).doubleValue();
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("Unsupported cast from type: " + inputType + " to INTEGER.");
    }
  }

  @ScalarFunction
  public static BigDecimal castBigDecimal(Object inputValue, String inputType) {
    FieldSpec.DataType inputDataType = FieldSpec.DataType.valueOf(inputType);
    switch (inputDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return BigDecimal.valueOf(((Number) inputValue).doubleValue());
      case BIG_DECIMAL:
        return (BigDecimal) inputValue;
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("Unsupported cast from type: " + inputType + " to INTEGER.");
    }
  }
}
