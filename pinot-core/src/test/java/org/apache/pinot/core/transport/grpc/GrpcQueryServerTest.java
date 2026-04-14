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
package org.apache.pinot.core.transport.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


/// Tests that {@link GrpcQueryServer} (SSE path) does not touch MSE-specific metrics.
public class GrpcQueryServerTest {

  private GrpcQueryServer _grpcQueryServer;

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));

    GrpcConfig grpcConfig = new GrpcConfig(GrpcConfig.DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, true);
    // Use port 0 so the OS picks a free port; we don't actually need the network for this test.
    _grpcQueryServer = new GrpcQueryServer("testServer", 0, grpcConfig, null,
        mock(QueryExecutor.class), mock(AccessControl.class), ThreadAccountantUtils.getNoOpAccountant());
    _grpcQueryServer.start();
  }

  @AfterClass
  public void tearDown() {
    if (_grpcQueryServer != null) {
      _grpcQueryServer.shutdown();
    }
    ServerMetrics.deregister();
  }

  /// Calls {@link GrpcQueryServer#submit} directly (bypassing gRPC transport) and verifies that
  /// {@link ServerMeter#MSE_QUERIES} is never touched by the SSE path.
  @Test
  public void testMseQueriesNotIncrementedBySsePath() {
    long mseCountBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    long grpcCountBefore = ServerMetrics.get().getMeteredValue(ServerMeter.GRPC_QUERIES).count();

    // Call submit() directly on the GrpcQueryServer instance — no network needed.
    // The mock AccessControl.hasDataAccess() returns false (Mockito default), so the request
    // is rejected early, but GRPC_QUERIES is still incremented first.
    _grpcQueryServer.submit(ServerRequest.newBuilder()
        .setSql("SELECT 1 FROM myTable")
        .build(), noopObserver());

    assertEquals(ServerMetrics.get().getMeteredValue(ServerMeter.GRPC_QUERIES).count(), grpcCountBefore + 1,
        "GRPC_QUERIES should be incremented by the SSE path");
    assertEquals(ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count(), mseCountBefore,
        "MSE_QUERIES should NOT be incremented by the SSE path");
  }

  private static StreamObserver<ServerResponse> noopObserver() {
    return new StreamObserver<ServerResponse>() {
      @Override public void onNext(ServerResponse value) { }
      @Override public void onError(Throwable t) { }
      @Override public void onCompleted() { }
    };
  }
}
