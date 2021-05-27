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
package org.apache.pinot.hadoop.job.partitioners;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.partition.ByteArrayPartitionFunction;
import org.apache.pinot.segment.spi.partition.HashCodePartitionFunction;
import org.apache.pinot.segment.spi.partition.ModuloPartitionFunction;
import org.apache.pinot.segment.spi.partition.MurmurPartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


public class PartitionFunctionFactory {

  private PartitionFunctionFactory() {
  }

  public enum PartitionFunctionType {
    Modulo, Murmur, HashCode, ByteArray;
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
   * This method generates and returns a partition function based on the provided string.
   *
   * @param functionName Name of partition function
   * @return Partition function
   */
  public static PartitionFunction getPartitionFunction(String functionName, int numPartitions) {

    // Return default partition function if there is no configuration given.
    if (functionName == null) {
      return new MurmurPartitionFunction(numPartitions);
    }

    PartitionFunctionType functionType;
    try {
      functionType = PartitionFunctionType.fromString(functionName);
    } catch (IllegalArgumentException e) {
      // By default, we use murmur
      functionType = PartitionFunctionType.Murmur;
    }

    switch (functionType) {
      case Modulo:
        return new ModuloPartitionFunction(numPartitions);

      case Murmur:
        return new MurmurPartitionFunction(numPartitions);

      case HashCode:
        return new HashCodePartitionFunction(numPartitions);

      case ByteArray:
        return new ByteArrayPartitionFunction(numPartitions);

      default:
        return new MurmurPartitionFunction(numPartitions);
    }
  }
}
