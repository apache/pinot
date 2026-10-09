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
package org.apache.pinot.query.service.dispatch;

import io.grpc.Deadline;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the max size of the messages the broker's dispatch channels accept from the servers.
public class DispatchClientTest {
  // Larger than the 4 MB gRPC accepts by default
  private static final String LARGE_VALUE = "x".repeat(5 * 1024 * 1024);

  private Server _server;
  private QueryServerInstance _serverInstance;

  @BeforeClass
  public void setUp()
      throws IOException {
    // Replies to every submit with a response larger than LARGE_VALUE
    _server = NettyServerBuilder.forPort(0).addService(new PinotQueryWorkerGrpc.PinotQueryWorkerImplBase() {
      @Override
      public void submit(Worker.QueryRequest request, StreamObserver<Worker.QueryResponse> responseObserver) {
        responseObserver.onNext(Worker.QueryResponse.newBuilder().putMetadata("value", LARGE_VALUE).build());
        responseObserver.onCompleted();
      }
    }).build().start();
    _serverInstance = new QueryServerInstance("server", "localhost", _server.getPort(), 0);
  }

  @AfterClass
  public void tearDown() {
    _server.shutdownNow();
  }

  @Test
  public void testDefaultMaxInboundMessageSize()
      throws Exception {
    QueryDispatcher queryDispatcher =
        new QueryDispatcher(mock(MailboxService.class), mock(FailureDetector.class), null, false,
            Duration.ofSeconds(1));
    try {
      AsyncResponse<Worker.QueryResponse> response = submit(queryDispatcher);
      assertNull(response.getThrowable());
      assertNotNull(response.getResponse());
      assertEquals(response.getResponse().getMetadataMap().get("value"), LARGE_VALUE);
    } finally {
      queryDispatcher.shutdown();
    }
  }

  @Test
  public void testConfiguredMaxInboundMessageSize()
      throws Exception {
    QueryDispatcher queryDispatcher = createQueryDispatcher(LARGE_VALUE.length() / 2);
    try {
      AsyncResponse<Worker.QueryResponse> response = submit(queryDispatcher);
      assertNull(response.getResponse());
      assertEquals(Status.fromThrowable(response.getThrowable()).getCode(), Status.Code.RESOURCE_EXHAUSTED);
    } finally {
      queryDispatcher.shutdown();
    }

    queryDispatcher = createQueryDispatcher(LARGE_VALUE.length() * 2);
    try {
      AsyncResponse<Worker.QueryResponse> response = submit(queryDispatcher);
      assertNull(response.getThrowable());
      assertNotNull(response.getResponse());
      assertEquals(response.getResponse().getMetadataMap().get("value"), LARGE_VALUE);
    } finally {
      queryDispatcher.shutdown();
    }
  }

  @Test
  public void testRejectsNonPositiveMaxInboundMessageSize() {
    for (int dispatchMaxInboundMessageSizeBytes : new int[]{0, -1}) {
      IllegalArgumentException exception =
          expectThrows(IllegalArgumentException.class, () -> createQueryDispatcher(dispatchMaxInboundMessageSizeBytes));
      assertTrue(exception.getMessage()
          .contains(CommonConstants.MultiStageQueryRunner.KEY_OF_DISPATCH_CHANNEL_MAX_INBOUND_MESSAGE_SIZE_BYTES));
    }
  }

  private static QueryDispatcher createQueryDispatcher(int dispatchMaxInboundMessageSizeBytes) {
    return new QueryDispatcher(mock(MailboxService.class), mock(FailureDetector.class), null, false,
        Duration.ofSeconds(1), 0, 0, false, false, CommonConstants.Broker.DEFAULT_STREAM_STATS_DRAIN_MS, false,
        dispatchMaxInboundMessageSizeBytes);
  }

  private AsyncResponse<Worker.QueryResponse> submit(QueryDispatcher queryDispatcher)
      throws Exception {
    CompletableFuture<AsyncResponse<Worker.QueryResponse>> future = new CompletableFuture<>();
    queryDispatcher.getOrCreateDispatchClient(_serverInstance)
        .submit(Worker.QueryRequest.getDefaultInstance(), _serverInstance, Deadline.after(10, TimeUnit.SECONDS),
            future::complete);
    return future.get(10, TimeUnit.SECONDS);
  }
}
