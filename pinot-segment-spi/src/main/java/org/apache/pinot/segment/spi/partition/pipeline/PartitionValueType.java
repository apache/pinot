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
package org.apache.pinot.segment.spi.partition.pipeline;


/**
 * Value types supported by the partition-pipeline compiler.
 */
public enum PartitionValueType {
  STRING,
  BYTES,
  INT,
  LONG,
  FLOAT,
  DOUBLE;

  public boolean isIntegral() {
    return this == INT || this == LONG;
  }

  public boolean isNumeric() {
    return isIntegral() || this == FLOAT || this == DOUBLE;
  }

  public static PartitionValueType fromJavaType(Class<?> clazz) {
    if (clazz == String.class) {
      return STRING;
    }
    if (clazz == byte[].class) {
      return BYTES;
    }
    if (clazz == int.class || clazz == Integer.class) {
      return INT;
    }
    if (clazz == long.class || clazz == Long.class) {
      return LONG;
    }
    if (clazz == float.class || clazz == Float.class) {
      return FLOAT;
    }
    if (clazz == double.class || clazz == Double.class) {
      return DOUBLE;
    }
    throw new IllegalArgumentException("Unsupported partition pipeline java type: " + clazz.getName());
  }
}
