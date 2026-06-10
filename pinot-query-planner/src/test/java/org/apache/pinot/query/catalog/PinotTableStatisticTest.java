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
package org.apache.pinot.query.catalog;

import org.apache.pinot.query.planner.spi.stats.NoOpStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Unit tests for {@link PinotTable#getStatistic()} — verifies that row-count statistics are
 * surfaced to the Calcite planner correctly based on provider output and confidence level.
 */
public class PinotTableStatisticTest {

  private static final String TABLE_NAME = "myTable";

  private static Schema buildMinimalSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  private static PinotTable tableWithProvider(PinotStatisticsProvider provider) {
    return new PinotTable(buildMinimalSchema(), false, TABLE_NAME, provider);
  }

  // -------------------------------------------------------------------------
  // Positive cases: stats present with sufficient confidence
  // -------------------------------------------------------------------------

  @Test
  public void testExactRowCountIsExposed() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    TableStatistics stats = TableStatistics.builder()
        .rowCount(1000, StatConfidence.EXACT)
        .build();
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(stats);

    PinotTable table = tableWithProvider(provider);
    assertEquals(table.getStatistic().getRowCount(), (Double) 1000.0,
        "EXACT row count must be surfaced to the planner");
  }

  @Test
  public void testEstimatedRowCountIsExposed() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    TableStatistics stats = TableStatistics.builder()
        .rowCount(500, StatConfidence.ESTIMATED)
        .build();
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(stats);

    PinotTable table = tableWithProvider(provider);
    assertEquals(table.getStatistic().getRowCount(), (Double) 500.0,
        "ESTIMATED row count must be surfaced to the planner");
  }

  // -------------------------------------------------------------------------
  // Negative cases: stats absent or confidence too low
  // -------------------------------------------------------------------------

  @Test
  public void testNullStatsReturnsUnknown() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(null);

    PinotTable table = tableWithProvider(provider);
    assertNull(table.getStatistic().getRowCount(),
        "Null stats must produce unknown row count (null)");
  }

  @Test
  public void testLowConfidenceReturnsUnknown() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    TableStatistics stats = TableStatistics.builder()
        .rowCount(999, StatConfidence.LOW)
        .build();
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(stats);

    PinotTable table = tableWithProvider(provider);
    assertNull(table.getStatistic().getRowCount(),
        "LOW confidence must be treated as unknown (null) — too biased for CBO");
  }

  @Test
  public void testUnknownConfidenceReturnsUnknown() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    TableStatistics stats = TableStatistics.builder()
        .rowCount(999, StatConfidence.UNKNOWN)
        .build();
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(stats);

    PinotTable table = tableWithProvider(provider);
    assertNull(table.getStatistic().getRowCount(),
        "UNKNOWN confidence must produce unknown row count (null)");
  }

  @Test
  public void testNegativeRowCountReturnsUnknown() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    TableStatistics stats = TableStatistics.builder()
        .rowCount(-1, StatConfidence.EXACT)
        .build();
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(stats);

    PinotTable table = tableWithProvider(provider);
    assertNull(table.getStatistic().getRowCount(),
        "rowCount = -1 (unknown sentinel) must produce unknown row count (null)");
  }

  @Test
  public void testNoOpStatisticsProviderReturnsUnknown() {
    PinotTable table = tableWithProvider(NoOpStatisticsProvider.INSTANCE);
    assertNull(table.getStatistic().getRowCount(),
        "NoOpStatisticsProvider must produce unknown row count (null)");
  }

  @Test
  public void testNullTableNameReturnsUnknown() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    // tableName == null: constructed without a name
    PinotTable table = new PinotTable(buildMinimalSchema(), false, null, provider);
    assertNull(table.getStatistic().getRowCount(),
        "Null table name must return unknown statistic");
  }
}
