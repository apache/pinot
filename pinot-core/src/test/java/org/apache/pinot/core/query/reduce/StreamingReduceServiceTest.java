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
package org.apache.pinot.core.query.reduce;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;


public class StreamingReduceServiceTest {

  @Test
  public void testThreadExceptionTransfer() {
    // simulate a thread exception in gRPC call and verify that the thread can transfer the exception
    Iterator<Server.ServerResponse> mockedResponse = (Iterator<Server.ServerResponse>) mock(Iterator.class);
    when(mockedResponse.hasNext()).thenReturn(true);
    String exceptionMessage = "Some exception";
    RuntimeException innerException = new RuntimeException(exceptionMessage);
    when(mockedResponse.next()).thenThrow(innerException);
    ExecutorService threadPoolService = Executors.newFixedThreadPool(1);
    ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 9527, TableType.OFFLINE);
    // supposedly we can use TestNG's annotation like @Test(expectedExceptions = { IOException.class }) to verify
    // here we hope to verify deeper to make sure the thrown exception is nested inside the exception
    assertTrue(verifyException(() -> {
          StreamingReduceService.processIterativeServerResponse(mock(StreamingReducer.class),
              threadPoolService,
              ImmutableMap.of(routingInstance, mockedResponse),
              1000,
              mock(ExecutionStatsAggregator.class));
          return null;
        }, cause -> cause.getMessage().contains(exceptionMessage))
    );
  }

  @Test
  public void testExecutionTimeout()
      throws Exception {
    // simulate a thread timeout in gRPC call and verify that the thread can transfer the exception
    Iterator<Server.ServerResponse> mockedResponse = (Iterator<Server.ServerResponse>) mock(Iterator.class);
    when(mockedResponse.hasNext()).thenReturn(true);
    when(mockedResponse.next()).then(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        Thread.sleep(1000);
        return null;
      }
    });
    final ExecutorService threadPoolService = Executors.newFixedThreadPool(1);
    final ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 9527, TableType.OFFLINE);
    //We cannot use TestNG's annotation like @Test(expectedExceptions = { IOException.class }) to verify
    // because the Exception we hope to verify is nested inside the final exception.
    assertTrue(verifyException(() -> {
          StreamingReduceService.processIterativeServerResponse(mock(StreamingReducer.class),
              threadPoolService,
              ImmutableMap.of(routingInstance, mockedResponse),
              10,
              mock(ExecutionStatsAggregator.class));
          return null;
        },
        (cause) -> cause instanceof TimeoutException));
  }

  @Test
  public void testIgnoreMissingSegmentsFiltering() throws Exception {
    // Build a metadata-only DataTable with a SERVER_SEGMENT_MISSING exception encoded as a streaming response
    org.apache.pinot.common.utils.DataSchema schema =
        new org.apache.pinot.common.utils.DataSchema(new String[]{"col1"},
            new org.apache.pinot.common.utils.DataSchema.ColumnDataType[]{
                org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG});
    org.apache.pinot.core.common.datatable.DataTableBuilder builder =
        org.apache.pinot.core.common.datatable.DataTableBuilderFactory.getDataTableBuilder(schema);
    org.apache.pinot.common.datatable.DataTable dataTable = builder.build().toMetadataOnlyDataTable();
    dataTable.addException(org.apache.pinot.spi.exception.QueryErrorCode.SERVER_SEGMENT_MISSING,
        "1 segments [segA] missing on server: Server_localhost_9527");
    // Set a request id in metadata so routing handling can route to the active async response
    dataTable.getMetadata().put(org.apache.pinot.common.datatable.DataTable.MetadataKey.REQUEST_ID.getName(), "1");
    byte[] payload = dataTable.toBytes();

    // Mock one server streaming response yielding the metadata-only block
    Iterator<Server.ServerResponse> mockedResponse = (Iterator<Server.ServerResponse>) mock(Iterator.class);
    when(mockedResponse.hasNext()).thenReturn(true, false);
    Server.ServerResponse resp = Server.ServerResponse.newBuilder()
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build();
    when(mockedResponse.next()).thenReturn(resp);

    // Prepare inputs for reduceOnStreamResponse
    StreamingReduceService service = new StreamingReduceService(
        new org.apache.pinot.spi.env.PinotConfiguration(java.util.Map.of()));
    org.apache.pinot.common.request.BrokerRequest brokerRequest =
        org.apache.pinot.sql.parsers.CalciteSqlCompiler.compileToBrokerRequest(
            "SELECT col1 FROM testTable LIMIT 1");
    // Set the query option to ignore missing segments
    brokerRequest.getPinotQuery().putToQueryOptions(
        org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.IGNORE_MISSING_SEGMENTS, "true");

    java.util.Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponseMap = java.util.Map.of(
        new ServerRoutingInstance("localhost", 9527, TableType.OFFLINE), mockedResponse);

    org.apache.pinot.common.metrics.BrokerMetrics metrics = mock(org.apache.pinot.common.metrics.BrokerMetrics.class);
    // Execute
    org.apache.pinot.common.response.broker.BrokerResponseNative response = service.reduceOnStreamResponse(
        brokerRequest, serverResponseMap, 1000, metrics);

    // Validate the SERVER_SEGMENT_MISSING was filtered out
    boolean hasMissing = response.getExceptions().stream().anyMatch(
        e -> e.getErrorCode() == org.apache.pinot.spi.exception.QueryErrorCode.SERVER_SEGMENT_MISSING.getId());
    org.testng.Assert.assertFalse(hasMissing);
  }

  private static boolean verifyException(Callable<Void> verifyTarget, Predicate<Throwable> verifyCause) {
    boolean exceptionVerified = false;
    if (verifyTarget == null || verifyCause == null) {
      throw new RuntimeException("verifyException method needs two non-null lambdas");
    }
    try {
      verifyTarget.call();
    } catch (Exception ex) {
      for (Throwable child = ex;
          child != null && child.getCause() != child && !exceptionVerified;
          child = child.getCause()) {
        exceptionVerified = verifyCause.test(child);
      }
    }
    return exceptionVerified;
  }
}
