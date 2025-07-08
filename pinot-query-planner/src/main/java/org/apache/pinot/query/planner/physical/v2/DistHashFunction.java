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
package org.apache.pinot.query.planner.physical.v2;

/**
 * Hash Functions supported by the v2 query optimizer. These are the hash functions supported by the Shuffle Exchange
 * runtime, and the ones which are considered for the table-scan. If there's a hash-function in the table-scan that
 * doesn't belong here, the table-scan won't be considered partitioned.
 */
public enum DistHashFunction {
  MURMUR,
  MURMUR3,
  HASHCODE,
  ABSHASHCODE;

  /**
   * Whether the given hash-function is considered by the v2 query optimizer for partitioning purposes.
   */
  public static boolean isSupported(String hashFunction) {
    try {
      DistHashFunction.valueOf(hashFunction.toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
