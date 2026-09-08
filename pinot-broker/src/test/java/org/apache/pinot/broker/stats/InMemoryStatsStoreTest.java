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
package org.apache.pinot.broker.stats;

import java.util.List;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// Runs the shared [StatsStore] contract against [InMemoryStatsStore], plus the one behavior that
/// distinguishes it: a new instance starts empty, so the caller re-collects everything.
public class InMemoryStatsStoreTest extends StatsStoreContractTest {

  @Override
  protected StatsStore createStore() {
    return new InMemoryStatsStore();
  }

  /// A fresh store keeps nothing from a previous one. Reconciliation relies on this: an empty
  /// crc map makes every segment look new, so the listener re-upserts the lot.
  @Test
  public void testNothingSurvivesANewInstance()
      throws Exception {
    _store.upsertSegmentStats("myTable_OFFLINE",
        List.of(new SegmentStatsRow("seg1", 42L, 500L, 2000L, 0L, 100L, false)));
    assertTrue(_store.getSegmentCrcs("myTable_OFFLINE").containsKey("seg1"));

    try (StatsStore fresh = createStore()) {
      fresh.init();
      assertTrue(fresh.getSegmentCrcs("myTable_OFFLINE").isEmpty(), "A new store must start empty");
    }
  }
}
