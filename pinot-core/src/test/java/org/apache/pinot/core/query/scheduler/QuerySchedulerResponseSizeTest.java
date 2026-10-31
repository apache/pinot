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
package org.apache.pinot.core.query.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests that [QueryScheduler] stops building a response that exceeds `maxServerResponseSizeBytes`, and replies with
/// an error instead.
public class QuerySchedulerResponseSizeTest {
  private static final PinotConfiguration CONFIG = new PinotConfiguration();
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.LONG});
  private static final int NUM_ROWS = 1000;

  private ServerMetrics _previousServerMetrics;
  private ServerMetrics _serverMetrics;
  private ExecutorService _executorService;

  /// [ServerMetrics#register] only swaps in against the NOOP default, so the instance that an earlier test class
  /// registered is cleared first, and restored in [#tearDown].
  @BeforeClass
  public void setUp() {
    _previousServerMetrics = ServerMetrics.get();
    ServerMetrics.deregister();
    _serverMetrics = mock(ServerMetrics.class);
    ServerMetrics.register(_serverMetrics);
    _executorService = Executors.newSingleThreadExecutor();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    _executorService.shutdownNow();
    ServerMetrics.deregister();
    ServerMetrics.register(_previousServerMetrics);
  }

  @Test
  public void testResponseWithinLimit()
      throws Exception {
    AtomicInteger numRowsWritten = new AtomicInteger();
    DataTable response = processQuery("OPTION(maxServerResponseSizeBytes=100000)", numRowsWritten);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(response.getNumberOfRows(), NUM_ROWS);
  }

  @Test
  public void testStopsBuildingResponseOverLimit()
      throws Exception {
    AtomicInteger numRowsWritten = new AtomicInteger();
    DataTable response = processQuery("OPTION(maxServerResponseSizeBytes=100)", numRowsWritten);
    String error = response.getExceptions().get(QueryErrorCode.QUERY_CANCELLATION.getId());
    assertNotNull(error, response.getExceptions().toString());
    assertEquals(error,
        "Serialized query response size of at least 104 exceeds threshold 100 for requestId 123 from broker unknown");
    // Each row is 8 bytes, so the builder stops at the 13th row instead of writing all of them.
    assertEquals(numRowsWritten.get(), 13);
    verify(_serverMetrics).addMeteredTableValue(TABLE_NAME, ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, 1);
    verify(_serverMetrics).addMeteredTableValue(TABLE_NAME, ServerMeter.QUERY_RESPONSE_SIZE, 104);
  }

  /// The rows fit in the limit, but the header, the data schema and the metadata take the response over it.
  @Test
  public void testRejectsSerializedResponseOverLimit()
      throws Exception {
    AtomicInteger numRowsWritten = new AtomicInteger();
    // The rows are exactly 8000 bytes.
    DataTable response = processQuery("OPTION(maxServerResponseSizeBytes=8000)", numRowsWritten);
    String error = response.getExceptions().get(QueryErrorCode.QUERY_CANCELLATION.getId());
    assertNotNull(error, response.getExceptions().toString());
    assertTrue(error.startsWith("Serialized query response size "), error);
    assertTrue(error.endsWith(" exceeds threshold 8000 for requestId 123 from broker unknown"), error);
    assertEquals(numRowsWritten.get(), NUM_ROWS);
    verify(_serverMetrics).addMeteredTableValue(TABLE_NAME, ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, 1);
  }

  @Test
  public void testNoLimitWithoutOption()
      throws Exception {
    AtomicInteger numRowsWritten = new AtomicInteger();
    DataTable response = processQuery("", numRowsWritten);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(response.getNumberOfRows(), NUM_ROWS);
  }

  private DataTable processQuery(String options, AtomicInteger numRowsWritten)
      throws Exception {
    clearInvocations(_serverMetrics);
    BaseResultsBlock resultsBlock = mock(BaseResultsBlock.class);
    when(resultsBlock.getDataTable()).thenAnswer(invocation -> {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(DATA_SCHEMA);
      for (int i = 0; i < NUM_ROWS; i++) {
        builder.startRow();
        builder.setColumn(0, (long) i);
        numRowsWritten.incrementAndGet();
        builder.finishRow();
      }
      return builder.build();
    });
    InstanceResponseBlock instanceResponse = new InstanceResponseBlock(resultsBlock);
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    when(queryExecutor.execute(any(), any())).thenReturn(instanceResponse);
    TestScheduler scheduler = new TestScheduler(queryExecutor);

    BrokerRequest brokerRequest = new BrokerRequest();
    String query = "SELECT value FROM " + TABLE_NAME + " " + options;
    brokerRequest.setPinotQuery(CalciteSqlParser.compileToPinotQuery(query));
    ServerQueryRequest queryRequest =
        new ServerQueryRequest(new InstanceRequest(123L, brokerRequest), _serverMetrics, System.currentTimeMillis());
    DataBuffer response = scheduler.processQueryAndSerializeResponse(queryRequest, _executorService);
    assertNotNull(response);
    byte[] bytes = new byte[Math.toIntExact(response.size())];
    response.copyTo(0, bytes);
    return DataTableFactory.getDataTable(bytes);
  }

  private static class TestScheduler extends QueryScheduler {
    TestScheduler(QueryExecutor queryExecutor) {
      super(CONFIG, "serverId", queryExecutor, ThreadAccountantUtils.getNoOpAccountant(),
          new LongAccumulator(Long::max, 0), new UnboundedResourceManager(CONFIG));
    }

    @Override
    public ListenableFuture<DataBuffer> submitResponse(ServerQueryRequest queryRequest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
      return "test";
    }
  }
}
