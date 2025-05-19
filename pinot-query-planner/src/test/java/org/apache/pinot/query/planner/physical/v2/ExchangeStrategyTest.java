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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ExchangeStrategyTest {
  @Test
  public void testGetRelDistribution() {
    // Ensure each exchange strategy can be mapped to a RelDistribution.
    for (ExchangeStrategy exchangeStrategy : ExchangeStrategy.values()) {
      List<Integer> keys = exchangeStrategy.isRequireKeys() ? List.of(1) : List.of();
      assertNotNull(ExchangeStrategy.getRelDistribution(exchangeStrategy, keys));
    }
    // Manual check for each exchange strategy.
    // Start with hash types
    List<ExchangeStrategy> hashExchangeStrategies = List.of(ExchangeStrategy.PARTITIONING_EXCHANGE,
        ExchangeStrategy.SUB_PARTITIONING_HASH_EXCHANGE, ExchangeStrategy.COALESCING_PARTITIONING_EXCHANGE);
    for (ExchangeStrategy exchangeStrategy : hashExchangeStrategies) {
      assertEquals(ExchangeStrategy.getRelDistribution(exchangeStrategy, List.of(1)).getType(),
          RelDistribution.Type.HASH_DISTRIBUTED);
    }
    // Singleton exchange
    assertEquals(ExchangeStrategy.getRelDistribution(ExchangeStrategy.SINGLETON_EXCHANGE, List.of()).getType(),
        RelDistribution.Type.SINGLETON);
    // Random exchange
    assertEquals(ExchangeStrategy.getRelDistribution(ExchangeStrategy.RANDOM_EXCHANGE, List.of()).getType(),
        RelDistribution.Type.RANDOM_DISTRIBUTED);
    // Broadcast exchange
    assertEquals(ExchangeStrategy.getRelDistribution(ExchangeStrategy.BROADCAST_EXCHANGE, List.of()).getType(),
        RelDistribution.Type.BROADCAST_DISTRIBUTED);
    // Sub-partitioning round robin exchange
    assertEquals(ExchangeStrategy.getRelDistribution(ExchangeStrategy.SUB_PARTITIONING_RR_EXCHANGE,
        List.of()).getType(), RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED);
  }

  @Test
  public void testGetRelDistributionInvalid() {
    // Test case when empty keys list but strategy requires keys
    assertThrows(IllegalStateException.class, () -> {
      ExchangeStrategy.getRelDistribution(ExchangeStrategy.PARTITIONING_EXCHANGE, List.of());
    });
    // Test case when non-empty keys list but strategy does not accept keys
    assertThrows(IllegalStateException.class, () -> {
      ExchangeStrategy.getRelDistribution(ExchangeStrategy.IDENTITY_EXCHANGE, List.of(1));
    });
  }
}
