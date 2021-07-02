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
package org.apache.pinot.hadoop.utils.preprocess;

import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pinot.spi.data.FieldSpec;


public class DataPreprocessingUtils {
  private DataPreprocessingUtils() {
  }

  /**
   * Converts a value into {@link WritableComparable} based on the given data type.
   * <p>NOTE: The passed in value must be either a Number or a String.
   */
  public static WritableComparable convertToWritableComparable(Object value, FieldSpec.DataType dataType) {
    if (value instanceof Number) {
      Number numberValue = (Number) value;
      switch (dataType) {
        case INT:
          return new IntWritable(numberValue.intValue());
        case LONG:
          return new LongWritable(numberValue.longValue());
        case FLOAT:
          return new FloatWritable(numberValue.floatValue());
        case DOUBLE:
          return new DoubleWritable(numberValue.doubleValue());
        case STRING:
          return new Text(numberValue.toString());
        default:
          throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }
    } else if (value instanceof String) {
      String stringValue = (String) value;
      switch (dataType) {
        case INT:
          return new IntWritable(Integer.parseInt(stringValue));
        case LONG:
          return new LongWritable(Long.parseLong(stringValue));
        case FLOAT:
          return new FloatWritable(Float.parseFloat(stringValue));
        case DOUBLE:
          return new DoubleWritable(Double.parseDouble(stringValue));
        case STRING:
          return new Text(stringValue);
        default:
          throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Value: %s must be either a Number or a String, found: %s", value, value.getClass()));
    }
  }

  public static void getOperations(Set<Operation> operationSet, String preprocessingOperationsString) {
    String[] preprocessingOpsArray = preprocessingOperationsString.split(",");
    for (String preprocessingOps : preprocessingOpsArray) {
      String trimmedOps = preprocessingOps.trim().toUpperCase();
      if (!trimmedOps.isEmpty()) {
        operationSet.add(Operation.getOperation(trimmedOps));
      }
    }
  }

  public enum Operation {
    PARTITION,
    SORT,
    RESIZE;

    public static Operation getOperation(String operationString) {
      for (Operation operation : Operation.values()) {
        if (operation.name().equals(operationString)) {
          return operation;
        }
      }
      throw new IllegalArgumentException("Unsupported data preprocessing operation: " + operationString);
    }
  }
}
