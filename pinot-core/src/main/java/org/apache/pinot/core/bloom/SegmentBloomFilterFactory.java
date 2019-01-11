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
package org.apache.pinot.core.bloom;

/**
 * Factory for bloom filter
 */
public class SegmentBloomFilterFactory {

  /**
   * Factory used when creating a new bloom filter
   *
   * @param cardinality cardinality of column
   * @param maxFalsePosProbability maximum false positive probability
   * @return a bloom filter
   */
  public static BloomFilter createSegmentBloomFilter(int cardinality, double maxFalsePosProbability) {
    // TODO: when we add more types of bloom filter, we will need to add a new config and wire in here
    return new GuavaOnHeapBloomFilter(cardinality, maxFalsePosProbability);
  }

  /**
   * Factory used when deserializing a bloom filter
   *
   * @param type a bloom filter type
   * @return a bloom filter based on the given type
   */
  public static BloomFilter createSegmentBloomFilter(BloomFilterType type) {
    switch (type) {
      case GUAVA_ON_HEAP:
        return new GuavaOnHeapBloomFilter();
    }
    throw new RuntimeException("Invalid bloom filter type: " + type.toString());
  }
}
