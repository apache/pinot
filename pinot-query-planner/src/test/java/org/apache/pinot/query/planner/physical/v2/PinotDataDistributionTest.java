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

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotDataDistributionTest {
  private static final String MURMUR_HASH_FUNCTION = "murmur";

  @Test
  public void testValidations() {
    {
      // Case-1: Singleton distribution with multiple workers.
      try {
        new PinotDataDistribution(RelDistribution.Type.SINGLETON, ImmutableList.of("0@0", "1@0"), 0L, null, null);
        fail();
      } catch (IllegalStateException ignored) {
      }
    }
    {
      // Case-2: Hash distribution with empty hash distribution desc.
      try {
        new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED, ImmutableList.of("0@0"), 0L,
            Collections.emptySet(), null);
        fail();
      } catch (IllegalStateException ignored) {
      }
    }
  }

  @Test
  public void testSatisfiesDistribution() {
    {
      // Case-1: Singleton constraint / Singleton actual distribution.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", null);
      assertTrue(distribution.satisfies(RelDistributions.SINGLETON));
    }
    {
      // Case-2: Singleton constraint / Non-singleton actual distribution.
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          ImmutableList.of("0@0", "1@0"), 0L, null, null);
      assertFalse(distribution.satisfies(RelDistributions.SINGLETON));
    }
    {
      // Case-3: Broadcast constraint / Broadcast distribution across multiple workers.
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          ImmutableList.of("0@0", "1@0"), 0L, null, null);
      assertTrue(distribution.satisfies(RelDistributions.BROADCAST_DISTRIBUTED));
    }
    {
      // Case-4: Broadcast constraint / Non-broadcast distribution but single worker.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", null);
      assertTrue(distribution.satisfies(RelDistributions.BROADCAST_DISTRIBUTED));
    }
    {
      // Case-5: Any constraint / Any distribution
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.ANY,
          ImmutableList.of("0@0", "1@0"), 0L, null, null);
      assertTrue(distribution.satisfies(RelDistributions.ANY));
    }
    {
      // Case-6: Any constraint / Non-any distribution
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          ImmutableList.of("0@0", "1@0"), 0L, null, null);
      assertTrue(distribution.satisfies(RelDistributions.ANY));
    }
    {
      // Case-7: Hash constraint / Hash distribution
      final List<Integer> keys = ImmutableList.of(1, 3);
      final int numPartitions = 8;
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
          ImmutableList.of("0@0", "1@0"), 0L, Collections.singleton(
              new HashDistributionDesc(keys, MURMUR_HASH_FUNCTION, numPartitions)), null);
      assertTrue(distribution.satisfies(RelDistributions.hash(keys)));
    }
    {
      // Case-8: Hash constraint / Non-hash distribution across multiple workers
      final List<Integer> keys = ImmutableList.of(1, 3);
      final int numPartitions = 8;
      PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          ImmutableList.of("0@0", "1@0"), 0L, null, null);
      assertFalse(distribution.satisfies(RelDistributions.hash(keys)));
    }
    {
      // Case-9: Hash constraint / Non-hash distribution but single worker
      final List<Integer> keys = ImmutableList.of(1, 3);
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", null);
      assertTrue(distribution.satisfies(RelDistributions.hash(keys)));
    }
  }

  @Test
  public void testSatisfiesHashDistributionDesc() {
    PinotDataDistribution distribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        ImmutableList.of("0@0", "1@0"), 0L, Collections.singleton(
            new HashDistributionDesc(ImmutableList.of(1, 3), MURMUR_HASH_FUNCTION, 8)), null);
    {
      // Case-1: Hash distribution desc with different keys.
      assertNull(distribution.satisfiesHashDistributionDesc(ImmutableList.of(1, 2)));
    }
    {
      // Case-2: Hash distribution desc with same keys, hash function and number of partitions.
      assertNotNull(distribution.satisfiesHashDistributionDesc(ImmutableList.of(1, 3)));
    }
  }

  @Test
  public void testSatisfiesCollation() {
    final RelFieldCollation exampleCollation = new RelFieldCollation(3, RelFieldCollation.Direction.ASCENDING,
        RelFieldCollation.NullDirection.FIRST);
    {
      // Case-1: No collation constraint / No collation in actual distribution.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", null);
      assertTrue(distribution.satisfies((RelCollation) null));
    }
    {
      // Case-2: No collation constraint / Collation in actual distribution.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", RelCollations.of(exampleCollation));
      assertTrue(distribution.satisfies((RelCollation) null));
    }
    {
      // Case-3: Collation constraint / No collation in actual distribution.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0", null);
      assertFalse(distribution.satisfies(RelCollations.of(exampleCollation)));
    }
    {
      // Case-4: Collation constraint / Collation in actual distribution.
      PinotDataDistribution distribution = PinotDataDistribution.singleton("0@0",
          RelCollations.of(exampleCollation));
      assertTrue(distribution.satisfies(RelCollations.of(exampleCollation)));
    }
  }
}
