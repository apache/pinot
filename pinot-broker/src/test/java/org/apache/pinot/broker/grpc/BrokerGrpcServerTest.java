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
package org.apache.pinot.broker.grpc;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BrokerGrpcServerTest {

  @Mock
  private BrokerRequestHandler _brokerRequestHandler;
  @Mock
  private BrokerMetrics _brokerMetrics;
  @Mock
  private SqlQueryExecutor _sqlQueryExecutor;

  private BrokerGrpcServer _brokerGrpcServer;
  private int _grpcPort;

  @BeforeMethod
  public void setUp()
      throws IOException {
    MockitoAnnotations.openMocks(this);

    // Find an available port
    try (ServerSocket socket = new ServerSocket(0)) {
      _grpcPort = socket.getLocalPort();
    }

    // Create config with gRPC port so server is created
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, _grpcPort);
    _brokerGrpcServer =
        new BrokerGrpcServer(config, "testBroker", _brokerMetrics, _brokerRequestHandler, _sqlQueryExecutor);
  }

  @AfterMethod
  public void tearDown() {
    if (_brokerGrpcServer != null) {
      _brokerGrpcServer.shutdown();
    }
  }

  @Test
  public void testGrpcBytesSentMetricOnSqlParsingError() {
    // Create a request with invalid SQL that will fail parsing
    Broker.BrokerRequest request = Broker.BrokerRequest.newBuilder()
        .setSql("INVALID SQL @@@ SYNTAX ERROR")
        .build();

    // Create a mock StreamObserver to capture responses
    List<Broker.BrokerResponse> responses = new ArrayList<>();
    StreamObserver<Broker.BrokerResponse> responseObserver = createMockStreamObserver(responses);

    // Submit the request - this will fail SQL parsing
    _brokerGrpcServer.submit(request, responseObserver);

    // Verify that one response was sent
    assertEquals(responses.size(), 1, "Should have sent exactly one error response");

    // Verify GRPC_BYTES_SENT metric was recorded
    ArgumentCaptor<Long> sizeCaptor = ArgumentCaptor.forClass(Long.class);
    verify(_brokerMetrics).addMeteredGlobalValue(eq(BrokerMeter.GRPC_BYTES_SENT), sizeCaptor.capture());

    // Verify the recorded size matches the actual response size
    long expectedSize = responses.get(0).getSerializedSize();
    assertEquals(sizeCaptor.getValue().longValue(), expectedSize,
        "GRPC_BYTES_SENT should record the actual response size");
    assertTrue(expectedSize > 0, "Response size should be greater than 0");
  }

  @Test
  public void testGrpcBytesSentMetricOnEmptyResult()
      throws Exception {
    // Create a valid SQL request
    Broker.BrokerRequest request = Broker.BrokerRequest.newBuilder()
        .setSql("SELECT * FROM testTable")
        .build();

    // Mock the request handler to return an empty result (no ResultTable)
    BrokerResponseNative emptyResponse = new BrokerResponseNative();
    when(_brokerRequestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenReturn(emptyResponse);

    // Create a mock StreamObserver to capture responses
    List<Broker.BrokerResponse> responses = new ArrayList<>();
    StreamObserver<Broker.BrokerResponse> responseObserver = createMockStreamObserver(responses);

    // Submit the request
    _brokerGrpcServer.submit(request, responseObserver);

    // Verify that one response was sent (empty/null result case)
    assertEquals(responses.size(), 1, "Should have sent exactly one response for empty result");

    // Verify GRPC_BYTES_SENT metric was recorded
    ArgumentCaptor<Long> sizeCaptor = ArgumentCaptor.forClass(Long.class);
    verify(_brokerMetrics).addMeteredGlobalValue(eq(BrokerMeter.GRPC_BYTES_SENT), sizeCaptor.capture());

    // Verify the recorded size matches the actual response size
    long expectedSize = responses.get(0).getSerializedSize();
    assertEquals(sizeCaptor.getValue().longValue(), expectedSize,
        "GRPC_BYTES_SENT should record the actual response size");
    assertTrue(expectedSize > 0, "Response size should be greater than 0");
  }

  @Test
  public void testGrpcBytesSentMetricOnSuccessfulQuery()
      throws Exception {
    // Create a valid SQL request
    Broker.BrokerRequest request = Broker.BrokerRequest.newBuilder()
        .setSql("SELECT col1, col2 FROM testTable")
        .build();

    // Mock the request handler to return a response with data
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1", "col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"value1", 100});
    rows.add(new Object[]{"value2", 200});
    rows.add(new Object[]{"value3", 300});
    brokerResponse.setResultTable(new ResultTable(dataSchema, rows));

    when(_brokerRequestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenReturn(brokerResponse);

    // Create a mock StreamObserver to capture responses
    List<Broker.BrokerResponse> responses = new ArrayList<>();
    StreamObserver<Broker.BrokerResponse> responseObserver = createMockStreamObserver(responses);

    // Submit the request
    _brokerGrpcServer.submit(request, responseObserver);

    // Verify that multiple responses were sent (metadata + schema + data blocks)
    assertTrue(responses.size() >= 3,
        "Should have sent at least 3 responses: metadata, schema, and data block(s)");

    // Calculate total expected size
    long expectedTotalSize = 0;
    for (Broker.BrokerResponse response : responses) {
      expectedTotalSize += response.getSerializedSize();
    }

    // Verify GRPC_BYTES_SENT metric was recorded with total size
    ArgumentCaptor<Long> sizeCaptor = ArgumentCaptor.forClass(Long.class);
    verify(_brokerMetrics).addMeteredGlobalValue(eq(BrokerMeter.GRPC_BYTES_SENT), sizeCaptor.capture());

    assertEquals(sizeCaptor.getValue().longValue(), expectedTotalSize,
        "GRPC_BYTES_SENT should record the total response size across all blocks");
    assertTrue(expectedTotalSize > 0, "Total response size should be greater than 0");
  }

  @Test
  public void testGrpcBytesReceivedMetric()
      throws Exception {
    // Create a request
    Broker.BrokerRequest request = Broker.BrokerRequest.newBuilder()
        .setSql("SELECT * FROM testTable")
        .build();

    // Mock empty response
    when(_brokerRequestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenReturn(new BrokerResponseNative());

    List<Broker.BrokerResponse> responses = new ArrayList<>();
    StreamObserver<Broker.BrokerResponse> responseObserver = createMockStreamObserver(responses);

    // Submit the request
    _brokerGrpcServer.submit(request, responseObserver);

    // Verify both GRPC_BYTES_RECEIVED and GRPC_BYTES_SENT are tracked
    verify(_brokerMetrics).addMeteredGlobalValue(
        eq(BrokerMeter.GRPC_BYTES_RECEIVED), eq((long) request.getSerializedSize()));
    verify(_brokerMetrics).addMeteredGlobalValue(eq(BrokerMeter.GRPC_BYTES_SENT), anyLong());
  }

  @Test
  public void testMetadataQueryDelegatesToSqlQueryExecutor() {
    PinotConfiguration brokerConf = new PinotConfiguration();
    brokerConf.setProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, 12345);

    BrokerRequestHandler brokerRequestHandler = Mockito.mock(BrokerRequestHandler.class);
    SqlQueryExecutor sqlQueryExecutor = Mockito.mock(SqlQueryExecutor.class);

    BrokerResponseNative metadataResponse = new BrokerResponseNative();
    DataSchema dataSchema =
        new DataSchema(new String[]{"tableName"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    metadataResponse.setResultTable(new ResultTable(dataSchema, Collections.emptyList()));
    Mockito.when(sqlQueryExecutor.executeMetadataStatement(ArgumentMatchers.any(SqlNodeAndOptions.class),
        ArgumentMatchers.anyMap())).thenReturn(metadataResponse);

    BrokerGrpcServer grpcServer =
        new BrokerGrpcServer(brokerConf, "broker_0", BrokerMetrics.get(), brokerRequestHandler, sqlQueryExecutor);

    Broker.BrokerRequest request =
        Broker.BrokerRequest.newBuilder()
            .setSql("SHOW TABLES")
            .putMetadata(CommonConstants.Broker.Grpc.COMPRESSION, "NONE")
            .putMetadata(CommonConstants.Broker.Grpc.ENCODING, "JSON")
            .build();

    CollectingObserver observer = new CollectingObserver();
    grpcServer.submit(request, observer);

    Mockito.verify(sqlQueryExecutor, Mockito.times(1))
        .executeMetadataStatement(ArgumentMatchers.any(SqlNodeAndOptions.class), ArgumentMatchers.anyMap());
    Mockito.verifyNoInteractions(brokerRequestHandler);

    Assert.assertNull(observer._error);
    Assert.assertTrue(observer._completed);
    Assert.assertEquals(observer._responses.size(), 2, "Expected metadata and schema blocks only");
  }

  /**
   * Helper method to create a mock StreamObserver that captures all responses.
   */
  private StreamObserver<Broker.BrokerResponse> createMockStreamObserver(List<Broker.BrokerResponse> responses) {
    @SuppressWarnings("unchecked")
    StreamObserver<Broker.BrokerResponse> observer = mock(StreamObserver.class);
    doAnswer(invocation -> {
      responses.add(invocation.getArgument(0));
      return null;
    }).when(observer).onNext(any());
    return observer;
  }

  private static class CollectingObserver implements StreamObserver<Broker.BrokerResponse> {
    private final List<Broker.BrokerResponse> _responses = new ArrayList<>();
    private volatile boolean _completed;
    private volatile Throwable _error;

    @Override
    public void onNext(Broker.BrokerResponse value) {
      _responses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      _error = t;
    }

    @Override
    public void onCompleted() {
      _completed = true;
    }
  }
}
