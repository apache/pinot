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
package org.apache.pinot.broker.querylog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_DIR;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_STORAGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class QueryLogSystemTableTest {
  private QueryLogSystemTable _systemTable;
  private Path _tempDir;
  private long _testStartTimeMs;

  @BeforeMethod
  public void setUp() {
    _systemTable = QueryLogSystemTable.getInstance();
    _systemTable.reset();
    _tempDir = null;
    _testStartTimeMs = System.currentTimeMillis();
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED, true,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES, 1000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS, 10_000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT, 50));
    _systemTable.initIfNeeded(config);

    _systemTable.append(newRecord(1_000L, 1, "foo_OFFLINE", 150L, "select * from foo", 10));
    _systemTable.append(newRecord(2_000L, 2, "bar_OFFLINE", 75L, "select * from bar where slow", 5));
    _systemTable.append(newRecord(3_000L, 3, "foo_OFFLINE", 300L, "select * from foo where slow", 20));
  }

  @AfterMethod
  public void tearDown() {
    _systemTable.reset();
    if (_tempDir != null) {
      try {
        deleteDirectory(_tempDir);
      } catch (IOException e) {
        // ignore cleanup failures in tests
      }
      _tempDir = null;
    }
  }

  @Test
  public void testDefaultOrdering()
      throws Exception {
    SqlNodeAndOptions sql = CalciteSqlParser.compileToSqlNodeAndOptions(
        "SELECT requestId, timeMs FROM system.query_log");
    BrokerResponse response = _systemTable.handleIfSystemTable(sql);
    assertNotNull(response);
    ResultTable table = response.getResultTable();
    assertNotNull(table);
    List<Object[]> rows = table.getRows();
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0)[0], 3L);
    assertEquals(rows.get(1)[0], 2L);
    assertEquals(rows.get(2)[0], 1L);
  }

  @Test
  public void testFilterAndOrder()
      throws Exception {
    SqlNodeAndOptions sql = CalciteSqlParser.compileToSqlNodeAndOptions(
        "SELECT requestId, timeMs FROM system.query_log WHERE tableName = 'foo_OFFLINE' AND timeMs > 100 "
            + "ORDER BY timeMs ASC LIMIT 1");
    BrokerResponseNative response = (BrokerResponseNative) _systemTable.handleIfSystemTable(sql);
    assertNotNull(response);
    ResultTable table = response.getResultTable();
    assertEquals(table.getRows().size(), 1);
    assertEquals(table.getRows().get(0)[0], 1L);
    assertEquals(table.getRows().get(0)[1], 150L);
  }

  @Test
  public void testLikeAndOffset()
      throws Exception {
    SqlNodeAndOptions sql = CalciteSqlParser.compileToSqlNodeAndOptions(
        "SELECT requestId, query FROM system.query_log WHERE query LIKE '%slow%' ORDER BY timeMs DESC LIMIT 1"
            + " OFFSET 1");
    BrokerResponse response = _systemTable.handleIfSystemTable(sql);
    ResultTable table = response.getResultTable();
    assertEquals(table.getRows().size(), 1);
    assertEquals(table.getRows().get(0)[0], 2L);
    assertEquals(table.getRows().get(0)[1], "select * from bar where slow");
  }

  @Test
  public void testDiskBackedStorageRetention()
      throws Exception {
    _systemTable.reset();
    _tempDir = Files.createTempDirectory("query-log-disk");
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED, true,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_STORAGE, "disk",
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_DIR, _tempDir.toString(),
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES, 2_048,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES, 512,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES, 10_000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT, 10_000));
    _systemTable.initIfNeeded(config);
    for (int i = 0; i < 50; i++) {
      _systemTable.append(newRecord(5_000L + i, i, "foo_OFFLINE", 200L + i,
          "select * from foo where payload = '" + "x".repeat(200) + i + "'", 10 + i));
    }
    SqlNodeAndOptions selectAll = CalciteSqlParser.compileToSqlNodeAndOptions(
        "SELECT requestId FROM system.query_log ORDER BY requestId ASC");
    BrokerResponseNative response = (BrokerResponseNative) _systemTable.handleIfSystemTable(selectAll);
    ResultTable table = response.getResultTable();
    int rowCount = table.getRows().size();
    assertTrue(rowCount < 50, "disk retention should drop the oldest records");
    long lowestRequestId = (long) table.getRows().get(0)[0];

    _systemTable.reset();
    _systemTable.initIfNeeded(config);
    BrokerResponseNative responseAfterRestart = (BrokerResponseNative) _systemTable.handleIfSystemTable(selectAll);
    ResultTable restartedTable = responseAfterRestart.getResultTable();
    assertEquals(restartedTable.getRows().size(), rowCount);
    assertEquals(restartedTable.getRows().get(0)[0], lowestRequestId);
  }

  @Test
  public void testDiskBackedRowBasedRetention()
      throws Exception {
    _systemTable.reset();
    _tempDir = Files.createTempDirectory("query-log-disk-rows");
    // Set very large disk size so only row-based retention applies
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED, true,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_STORAGE, "disk",
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_DIR, _tempDir.toString(),
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES, 10_000_000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES, 512 * 1024,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES, 200,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS, 24 * 3600_000L,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT, 10_000));
    _systemTable.initIfNeeded(config);

    // Append 500 small records
    for (int i = 0; i < 500; i++) {
      _systemTable.append(newRecord(1_000L + i, i, "rows_OFFLINE", 10L,
          "select " + i, 1));
    }
    BrokerResponseNative response = (BrokerResponseNative) _systemTable.handleIfSystemTable(
        CalciteSqlParser.compileToSqlNodeAndOptions("SELECT requestId FROM system.query_log ORDER BY requestId ASC"));
    assertNotNull(response);
    int rowCount = response.getResultTable().getRows().size();
    assertEquals(rowCount, 200, "row-based retention should cap records at maxEntries");
    long lowestRequestId = (long) response.getResultTable().getRows().get(0)[0];

    // Restart and ensure retention persists
    _systemTable.reset();
    _systemTable.initIfNeeded(config);
    BrokerResponseNative responseAfterRestart = (BrokerResponseNative) _systemTable.handleIfSystemTable(
        CalciteSqlParser.compileToSqlNodeAndOptions("SELECT requestId FROM system.query_log ORDER BY requestId ASC"));
    ResultTable restartedTable = responseAfterRestart.getResultTable();
    assertEquals(restartedTable.getRows().size(), 200);
    assertEquals(restartedTable.getRows().get(0)[0], lowestRequestId);
  }

  @Test
  public void testDiskBackedTimeBasedRetention()
      throws Exception {
    _systemTable.reset();
    _tempDir = Files.createTempDirectory("query-log-disk-time");
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED, true,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_STORAGE, "disk",
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_DIR, _tempDir.toString(),
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES, 10_000_000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES, 512 * 1024,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES, 10_000,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS, 5_000L,
        CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT, 10_000));
    _systemTable.initIfNeeded(config);

    // One old record outside retention, two recent within retention
    _systemTable.append(newRecord(-10_000L, 1, "time_OFFLINE", 10L, "old", 1));
    _systemTable.append(newRecord(100L, 2, "time_OFFLINE", 10L, "new1", 1));
    _systemTable.append(newRecord(200L, 3, "time_OFFLINE", 10L, "new2", 1));

    BrokerResponseNative response = (BrokerResponseNative) _systemTable.handleIfSystemTable(
        CalciteSqlParser.compileToSqlNodeAndOptions(
            "SELECT requestId FROM system.query_log ORDER BY requestId ASC"));
    ResultTable table = response.getResultTable();
    assertEquals(table.getRows().size(), 2, "time-based retention should drop old records");
    long firstId = (long) table.getRows().get(0)[0];

    // Restart and ensure retention persists
    _systemTable.reset();
    _systemTable.initIfNeeded(config);
    BrokerResponseNative responseAfterRestart = (BrokerResponseNative) _systemTable.handleIfSystemTable(
        CalciteSqlParser.compileToSqlNodeAndOptions(
            "SELECT requestId FROM system.query_log ORDER BY requestId ASC"));
    ResultTable restarted = responseAfterRestart.getResultTable();
    assertEquals(restarted.getRows().size(), 2);
    assertEquals(restarted.getRows().get(0)[0], firstId);
  }

  private QueryLogRecord newRecord(long timestampOffset, long requestId, String tableName, long timeMs, String query,
      long numDocsScanned) {
    long timestamp = _testStartTimeMs + timestampOffset;
    DefaultRequestContext requestContext = new DefaultRequestContext();
    requestContext.setBrokerId("broker-0");
    requestContext.setRequestId(requestId);
    requestContext.setRequestArrivalTimeMillis(timestamp - 5);
    requestContext.setTableName(tableName);
    requestContext.setOfflineServerTenant("tenant_OFF");
    requestContext.setRealtimeServerTenant("tenant_RT");
    requestContext.setFanoutType(RequestContext.FanoutType.HYBRID);
    requestContext.setNumUnavailableSegments(1);
    requestContext.setQuery(query);

    BrokerResponseNative response = new BrokerResponseNative();
    response.setTimeUsedMs(timeMs);
    response.setBrokerReduceTimeMs(3);
    response.setNumDocsScanned(numDocsScanned);
    response.setTotalDocs(100);
    response.setNumEntriesScannedInFilter(2);
    response.setNumEntriesScannedPostFilter(4);
    response.setNumSegmentsQueried(5);
    response.setNumSegmentsProcessed(6);
    response.setNumSegmentsMatched(7);
    response.setNumConsumingSegmentsQueried(1);
    response.setNumConsumingSegmentsProcessed(1);
    response.setNumConsumingSegmentsMatched(1);
    response.setMinConsumingFreshnessTimeMs(50);
    response.setNumServersResponded(2);
    response.setNumServersQueried(2);
    response.setGroupsTrimmed(false);
    response.setNumGroupsLimitReached(false);
    response.setNumGroupsWarningLimitReached(false);
    response.setOfflineThreadCpuTimeNs(11);
    response.setOfflineSystemActivitiesCpuTimeNs(12);
    response.setOfflineResponseSerializationCpuTimeNs(13);
    response.setRealtimeThreadCpuTimeNs(15);
    response.setRealtimeSystemActivitiesCpuTimeNs(16);
    response.setRealtimeResponseSerializationCpuTimeNs(17);
    response.setOfflineThreadMemAllocatedBytes(19);
    response.setOfflineResponseSerMemAllocatedBytes(20);
    response.setRealtimeThreadMemAllocatedBytes(22);
    response.setRealtimeResponseSerMemAllocatedBytes(23);
    response.setPools(Set.of(1));
    response.setRLSFiltersApplied(false);
    response.setNumRowsResultSet(1);
    response.setTablesQueried(Set.of(tableName));
    response.setTraceInfo(Map.of("traceId", Long.toString(requestId)));
    response.setExceptions(Collections.emptyList());
    DataSchema schema = new DataSchema(new String[]{"dummy"}, new ColumnDataType[]{ColumnDataType.STRING});
    response.setResultTable(new ResultTable(schema, Collections.singletonList(new Object[]{"value"})));

    QueryLogger.QueryLogParams params = new QueryLogger.QueryLogParams(requestContext, tableName, response,
        QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, null, null);
    return QueryLogRecord.from(params, query, "10.0.0." + requestId, timestamp);
  }

  private static void deleteDirectory(Path directory)
      throws IOException {
    if (directory == null || Files.notExists(directory)) {
      return;
    }
    Files.walk(directory)
        .sorted(Comparator.reverseOrder())
        .forEach(path -> {
          try {
            Files.deleteIfExists(path);
          } catch (IOException ignored) {
          }
        });
  }
}
