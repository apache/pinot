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
package org.apache.pinot.segment.spi.partition;

import java.util.HashMap;
import java.util.Map;


/**
 * Factory to build instances of {@link PartitionFunction}.
 */
public class PartitionFunctionFactory {
  // Enum for various partition functions to be added.
  public enum PartitionFunctionType {
    Modulo, Murmur, Murmur3, ByteArray, HashCode, BoundedColumnValue;
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
   * @param functionName Name of partition function
   * @param numPartitions Number of partitions.
   * @param functionConfig The function configuration for given function.
   * @return Partition function
   */
  // TODO: introduce a way to inject custom partition function
  // a custom partition function could be used in the realtime stream partitioning or offline segment partitioning.
  // The PartitionFunctionFactory should be able to support these default implementations, as well as instantiate
  // based on config
  public static PartitionFunction getPartitionFunction(String functionName, int numPartitions,
      Map<String, String> functionConfig) {
    PartitionFunctionType function = PartitionFunctionType.fromString(functionName);
    switch (function) {
      case Modulo:
        return new ModuloPartitionFunction(numPartitions);

      case Murmur:
        return new MurmurPartitionFunction(numPartitions);

      case Murmur3:
        return new Murmur3PartitionFunction(numPartitions, functionConfig);

      case ByteArray:
        return new ByteArrayPartitionFunction(numPartitions);

      case HashCode:
        return new HashCodePartitionFunction(numPartitions);

      case BoundedColumnValue:
        return new BoundedColumnValuePartitionFunction(numPartitions, functionConfig);

      default:
        throw new IllegalArgumentException("Illegal partition function name: " + functionName);
    }
  }
}
