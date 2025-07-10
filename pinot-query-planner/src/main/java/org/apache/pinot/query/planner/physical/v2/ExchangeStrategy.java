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

import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;


/**
 * Defines how data is transferred across an Exchange.
 */
public enum ExchangeStrategy {
  /**
   * There's a single stream in the receiver, so each stream in the sender sends data to the same.
   */
  SINGLETON_EXCHANGE(false),
  /**
   * stream-ID X sends data to stream-ID X. This cannot be modeled by PARTITIONING_EXCHANGE because the fan-out for
   * this type of exchange is 1:1.
   */
  IDENTITY_EXCHANGE(false),
  /**
   * Each stream will partition the outgoing stream based on a set of keys and a hash function.
   * Fanout for this type of exchange is 1:all.
   */
  PARTITIONING_EXCHANGE(true),
  /**
   * stream-ID X will sub-partition: i.e. divide the stream so that the data is sent to the streams
   * {@code X, X + F, X + 2*F, ...}. Here F is the sub-partitioning factor. Records are assigned based on a
   * hash function. This is useful when joining two tables which have different number of partitions, but one of the
   * partition counts divides the other.
   * <b>Note:</b> This is different and better than partitioning exchange because the fanout is F, and not N * (N*F).
   */
  SUB_PARTITIONING_HASH_EXCHANGE(true),
  /**
   * Same as above but records are sub-partitioned in a round-robin way. This will increase parallelism but lose
   * data partitioning.
   */
  SUB_PARTITIONING_RR_EXCHANGE(false),
  /**
   * Similar to sub-partitioning, except it does the inverse and merges partitions. Partitions are merged in a way
   * that we still preserve partitions, but only change the partition count. i.e. if current partition count is 16,
   * and we want 8 partitions, then partition-0 in the receiver will receive data from partition-0 and partition-8
   * in the sender.
   */
  COALESCING_PARTITIONING_EXCHANGE(true),
  /**
   * Each stream will send data to all receiving streams.
   */
  BROADCAST_EXCHANGE(false),
  /**
   * Records are sent randomly from a given worker in the sender to some worker in the receiver.
   */
  RANDOM_EXCHANGE(false);

  /**
   * This is true when the Exchange Strategy is such that it requires a List&lt;Integer&gt; representing the
   * distribution keys. The list must be non-empty.
   */
  private final boolean _requireKeys;

  /**
   * See {@link #_requireKeys}.
   */
  public boolean isRequireKeys() {
    return _requireKeys;
  }

  ExchangeStrategy(boolean requireKeys) {
    _requireKeys = requireKeys;
  }

  public static RelDistribution getRelDistribution(ExchangeStrategy exchangeStrategy, List<Integer> keys) {
    if (exchangeStrategy.isRequireKeys() && keys.isEmpty()) {
      throw new IllegalStateException(String.format("ExchangeStrategy=%s requires distribution keys, but none found",
          exchangeStrategy));
    } else if (!exchangeStrategy.isRequireKeys() && !keys.isEmpty()) {
      throw new IllegalStateException(String.format(
          "ExchangeStrategy=%s does not require distribution keys but found %s", exchangeStrategy, keys));
    }
    switch (exchangeStrategy) {
      case PARTITIONING_EXCHANGE:
      case SUB_PARTITIONING_HASH_EXCHANGE:
      case COALESCING_PARTITIONING_EXCHANGE:
        return RelDistributions.hash(keys);
      case IDENTITY_EXCHANGE:
        return RelDistributions.hash(List.of());
      case BROADCAST_EXCHANGE:
        return RelDistributions.BROADCAST_DISTRIBUTED;
      case SINGLETON_EXCHANGE:
        return RelDistributions.SINGLETON;
      case SUB_PARTITIONING_RR_EXCHANGE:
        return RelDistributions.ROUND_ROBIN_DISTRIBUTED;
      case RANDOM_EXCHANGE:
        return RelDistributions.RANDOM_DISTRIBUTED;
      default:
        throw new IllegalStateException(String.format("Unexpected exchange strategy: %s", exchangeStrategy));
    }
  }
}
