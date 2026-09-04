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

import java.util.OptionalLong;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/// The adapter between the broker's collected statistics and the planner. It is a thin delegation,
/// but it is the seam the planner sees, so the delegation itself is what needs pinning: a wrong
/// table name or a swallowed empty would change every estimate silently.
public class BrokerStatisticsProviderTest {

  private static final String TABLE = "myTable_OFFLINE";

  private AutoCloseable _mocks;

  @Mock
  private BrokerTableStatsManager _statsManager;

  private BrokerStatisticsProvider _provider;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _provider = new BrokerStatisticsProvider(_statsManager);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testTableStatisticsAreForwardedUnchanged() {
    TableStatistics stats = TableStatistics.builder()
        .rowCount(500L, StatConfidence.EXACT)
        .tableSizeBytes(1024L, StatConfidence.EXACT)
        .build();
    when(_statsManager.getTableStats(TABLE)).thenReturn(stats);

    // Same instance, not a copy: confidence must survive the hop, since the planner drops LOW.
    assertSame(_provider.getTableStatistics(TABLE), stats);
    verify(_statsManager).getTableStats(TABLE);
  }

  @Test
  public void testAbsentTableStatisticsStayAbsent() {
    when(_statsManager.getTableStats(TABLE)).thenReturn(null);
    assertNull(_provider.getTableStatistics(TABLE));
  }

  @Test
  public void testTimeRangeEstimateIsForwardedWithItsBounds() {
    when(_statsManager.estimateRowsInTimeRange(TABLE, 100L, 200L)).thenReturn(OptionalLong.of(42L));
    OptionalLong estimate = _provider.estimateRowsInTimeRange(TABLE, 100L, 200L);
    assertEquals(estimate, OptionalLong.of(42L));
    verify(_statsManager).estimateRowsInTimeRange(TABLE, 100L, 200L);
  }

  @Test
  public void testUnknownTimeRangeEstimateStaysUnknown() {
    // Empty means "cannot estimate". Turning it into 0 here would tell the planner the range is
    // empty, which is the single most damaging thing this adapter could do.
    when(_statsManager.estimateRowsInTimeRange(TABLE, 0L, 1L)).thenReturn(OptionalLong.empty());
    assertFalse(_provider.estimateRowsInTimeRange(TABLE, 0L, 1L).isPresent());
  }

  @Test
  public void testColumnStatisticsAreNotCollectedYet() {
    // Deliberately unimplemented: no column statistics are collected at this point in the stack.
    // It must return null rather than reach the manager for data that does not exist.
    assertNull(_provider.getColumnStatistics(TABLE, "someColumn"));
    verifyNoMoreInteractions(_statsManager);
  }
}
