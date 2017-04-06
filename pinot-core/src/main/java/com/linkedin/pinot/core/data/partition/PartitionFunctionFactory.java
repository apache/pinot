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
package com.linkedin.pinot.core.data.partition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Factory to build instances of {@link PartitionFunction}.
 */
public class PartitionFunctionFactory {
  public static final String PARTITION_FUNCTION_DELIMITER = "\t\t";

  // Enum for various partition functions to be added.
  public enum PartitionFunctionType {
    Modulo,
    Murmur,
    ByteArray;
    // Add more functions here.

    private static final Map<String, PartitionFunctionType> VALUE_MAP = new HashMap<>();

    static {
      for (PartitionFunctionType functionType : PartitionFunctionType.values()) {
        VALUE_MAP.put(functionType.name().toLowerCase(), functionType);
      }
    }

    public static PartitionFunctionType fromString(String name) {
      PartitionFunctionType functionType = VALUE_MAP.get(name.toLowerCase());

      if (functionType == null) {
        throw new IllegalArgumentException("No enum constant for: " + name);
      }
      return functionType;
    }
  }

  /**
   * Private constructor so that the class cannot be instantiated.
   */
  private PartitionFunctionFactory() {
  }

  /**
   * This method generates and returns a partition function based on the provided string.
   *
   * @param partitionFunctionString String containing function name and its arguments.
   * @return Partition function
   */
  public static PartitionFunction getPartitionFunction(@Nonnull String partitionFunctionString) {
    String[] tokens = partitionFunctionString.split(PARTITION_FUNCTION_DELIMITER);
    String functionName = tokens[0];
    String[] args = Arrays.copyOfRange(tokens, 1, tokens.length);

    PartitionFunctionType function = PartitionFunctionType.fromString(functionName);
    switch (function) {
      case Modulo:
        return new ModuloPartitionFunction(args);

      case Murmur:
        return new MurmurPartitionFunction(args);

      case ByteArray:
        return new ByteArrayPartitionFunction(args);

      default:
        throw new IllegalArgumentException("Illegal partition function name: " + functionName);
    }
  }
}
