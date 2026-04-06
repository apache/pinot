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
package org.apache.pinot.core.query.killing;

import org.apache.pinot.spi.query.QueryScanCostContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link QueryKillReport}.
 */
public class QueryKillReportTest {

  @Test
  public void testSnapshotsValuesAtCreationTime() {
    QueryScanCostContext context = new QueryScanCostContext();
    context.addEntriesScannedInFilter(1000L);
    context.addDocsScanned(500L);
    context.addEntriesScannedPostFilter(200L);

    QueryKillReport report = new QueryKillReport(
        "queryId-1",
        "myTable_OFFLINE",
        "EntriesScannedInFilterStrategy",
        "numEntriesScannedInFilter",
        1000L,
        800L,
        "TABLE_CONFIG",
        context
    );

    // Mutate context after report creation — report must be unaffected
    context.addEntriesScannedInFilter(99999L);
    context.addDocsScanned(99999L);
    context.addEntriesScannedPostFilter(99999L);

    assertEquals(report.getSnapshotEntriesScannedInFilter(), 1000L);
    assertEquals(report.getSnapshotDocsScanned(), 500L);
    assertEquals(report.getSnapshotEntriesScannedPostFilter(), 200L);
  }

  @Test
  public void testCustomerMessageContainsAllFields() {
    QueryScanCostContext context = new QueryScanCostContext();
    context.addEntriesScannedInFilter(1234567L);
    context.addDocsScanned(100L);
    context.addEntriesScannedPostFilter(50L);

    QueryKillReport report = new QueryKillReport(
        "queryId-abc",
        "salesTable_REALTIME",
        "EntriesScannedInFilterStrategy",
        "numEntriesScannedInFilter",
        1234567L,
        1000000L,
        "CLUSTER_CONFIG",
        context
    );

    String msg = report.toCustomerMessage();

    assertTrue(msg.contains("salesTable_REALTIME"), "Should contain table name");
    assertTrue(msg.contains("numEntriesScannedInFilter"), "Should contain metric name");
    assertTrue(msg.contains("1,234,567"), "Should contain actual value with commas");
    assertTrue(msg.contains("1,000,000"), "Should contain threshold with commas");
    assertTrue(msg.contains("CLUSTER_CONFIG"), "Should contain config source");
    assertTrue(msg.contains("missing index") || msg.contains("index"),
        "Should include advice about missing indexes");
  }

  @Test
  public void testInternalLogMessageIsStructured() {
    QueryScanCostContext context = new QueryScanCostContext();
    context.addEntriesScannedInFilter(500L);
    context.addDocsScanned(300L);
    context.addEntriesScannedPostFilter(150L);

    QueryKillReport report = new QueryKillReport(
        "queryId-xyz",
        "ordersTable_OFFLINE",
        "DocsScannedStrategy",
        "numDocsScanned",
        500L,
        400L,
        "TABLE_CONFIG",
        context
    );

    String msg = report.toInternalLogMessage();

    assertTrue(msg.startsWith("QUERY_KILLED"), "Should start with QUERY_KILLED prefix");
    assertTrue(msg.contains("queryId=queryId-xyz"), "Should have queryId key=value");
    assertTrue(msg.contains("table=ordersTable_OFFLINE"), "Should have table key=value");
    assertTrue(msg.contains("metric=numDocsScanned"), "Should have metric key=value");
    // Internal log should use plain numbers, not comma-formatted
    assertTrue(msg.contains("actual=500"), "Should have actual key=value without commas");
    assertTrue(msg.contains("threshold=400"), "Should have threshold key=value without commas");
  }

  @Test
  public void testGetters() {
    QueryScanCostContext context = new QueryScanCostContext();
    context.addEntriesScannedInFilter(100L);
    context.addDocsScanned(50L);
    context.addEntriesScannedPostFilter(25L);

    QueryKillReport report = new QueryKillReport(
        "queryId-getters",
        "testTable_OFFLINE",
        "TestStrategy",
        "numEntriesScannedInFilter",
        100L,
        90L,
        "SERVER_CONFIG",
        context
    );

    assertEquals(report.getQueryId(), "queryId-getters");
    assertEquals(report.getTableName(), "testTable_OFFLINE");
    assertEquals(report.getStrategyName(), "TestStrategy");
    assertEquals(report.getTriggeringMetric(), "numEntriesScannedInFilter");
    assertEquals(report.getActualValue(), 100L);
    assertEquals(report.getThresholdValue(), 90L);
    assertEquals(report.getConfigSource(), "SERVER_CONFIG");
    assertEquals(report.getSnapshotEntriesScannedInFilter(), 100L);
    assertEquals(report.getSnapshotDocsScanned(), 50L);
    assertEquals(report.getSnapshotEntriesScannedPostFilter(), 25L);
    assertTrue(report.getElapsedTimeMs() >= 0L, "Elapsed time must be non-negative");
  }
}
