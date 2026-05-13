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
package org.apache.pinot.query.service.dispatch.streaming;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link StreamingDispatchObserver}. Drives ServerToBroker messages directly into
 * {@link StreamObserver#onNext} on a real {@link StreamingQuerySession} and verifies the right session methods are
 * called and the session completes when expected.
 */
public class StreamingDispatchObserverTest {

  /**
   * Happy path: submit_ack, then 2 OpChainCompletes, then ServerDone. ackCallback fires once with the response, the
   * session's latch drains to zero, and the stream is unregistered.
   */
  @Test
  public void testHappyPath()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);
    AtomicReference<Worker.QueryResponse> ackResponse = new AtomicReference<>();
    AtomicReference<Throwable> ackError = new AtomicReference<>();

    QueryServerInstance server = mockServer();
    StreamingDispatchObserver observer = new StreamingDispatchObserver(server, session, 2,
        (resp, err) -> {
          ackResponse.set(resp);
          ackError.set(err);
        });

    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);
    session.registerStream(observer);

    Worker.QueryResponse okResponse = Worker.QueryResponse.newBuilder().putMetadata("STATUS_OK", "").build();
    observer.onNext(Worker.ServerToBroker.newBuilder().setSubmitAck(okResponse).build());
    Assert.assertEquals(ackResponse.get(), okResponse);
    Assert.assertNull(ackError.get());

    observer.onNext(Worker.ServerToBroker.newBuilder().setOpchain(buildOpChainComplete(1, 0, 5)).build());
    observer.onNext(Worker.ServerToBroker.newBuilder().setOpchain(buildOpChainComplete(1, 1, 7)).build());

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS));

    observer.onNext(Worker.ServerToBroker.newBuilder().setDone(Worker.ServerDone.getDefaultInstance()).build());
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().get(1), 2);
  }

  /**
   * onError before submit_ack: ack callback receives the error (so the dispatcher's ack-await doesn't hang) and the
   * session latch drains by the per-server remaining-expected count.
   */
  @Test
  public void testErrorBeforeAckSurfacesToCallback()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 4);
    AtomicReference<Throwable> ackError = new AtomicReference<>();

    StreamingDispatchObserver observer = new StreamingDispatchObserver(mockServer(), session, 4,
        (resp, err) -> ackError.set(err));
    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);
    session.registerStream(observer);

    RuntimeException transport = new RuntimeException("transport fault");
    observer.onError(transport);

    Assert.assertSame(ackError.get(), transport);
    // Latch should be drained by remaining-expected (4 since none reported before error).
    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "expected session to complete after stream error drained latch");
  }

  /**
   * Cancel via the StreamingServerHandle interface writes a BrokerToServer.cancel onto the outbound stream.
   */
  @Test
  public void testCancelEmitsBrokerToServerCancel()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(42L, 0);
    StreamingDispatchObserver observer = new StreamingDispatchObserver(mockServer(), session, 0,
        (resp, err) -> { });

    @SuppressWarnings("unchecked")
    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);

    observer.cancel(42L);

    ArgumentCaptor<Worker.BrokerToServer> captor = ArgumentCaptor.forClass(Worker.BrokerToServer.class);
    Mockito.verify(outbound).onNext(captor.capture());
    Worker.BrokerToServer sent = captor.getValue();
    Assert.assertTrue(sent.hasCancel());
    Assert.assertEquals(sent.getCancel().getRequestId(), 42L);
  }

  /**
   * STATUS_ERROR ack followed by ServerDone with no opchains: the DONE handler must drain the remaining expected count
   * from the latch so {@link StreamingQuerySession#awaitCompletion} returns promptly, not at the query deadline.
   */
  @Test
  public void testDoneWithNoReportedOpChainsAfterErrorAckDrainsLatch()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);
    AtomicReference<Worker.QueryResponse> ackResponse = new AtomicReference<>();
    AtomicReference<Throwable> ackError = new AtomicReference<>();

    StreamingDispatchObserver observer = new StreamingDispatchObserver(mockServer(), session, 2,
        (resp, err) -> {
          ackResponse.set(resp);
          ackError.set(err);
        });
    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);
    session.registerStream(observer);

    Worker.QueryResponse errorResponse =
        Worker.QueryResponse.newBuilder().putMetadata("STATUS_ERROR", "submit failed").build();
    observer.onNext(Worker.ServerToBroker.newBuilder().setSubmitAck(errorResponse).build());
    Assert.assertEquals(ackResponse.get(), errorResponse);
    Assert.assertNull(ackError.get(), "STATUS_ERROR ack is not a transport error; ackError must be null");

    // Server sends DONE without any OpChainCompletes — 2 latch counts still outstanding
    observer.onNext(Worker.ServerToBroker.newBuilder().setDone(Worker.ServerDone.getDefaultInstance()).build());

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "awaitCompletion must return promptly after DONE drains remaining latch count");
    Assert.assertEquals(session.getOutstandingCount(), 0L);
  }

  /**
   * Partial reports + DONE (not onError): server reports 1 of 3 opchains then sends DONE early. The DONE handler must
   * drain only the remaining 2 counts, not all 3.
   */
  @Test
  public void testDoneAfterPartialOpChainsReportedDrainsRemaining()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 3);
    StreamingDispatchObserver observer = new StreamingDispatchObserver(mockServer(), session, 3,
        (resp, err) -> { });
    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);
    session.registerStream(observer);

    Worker.QueryResponse okResponse = Worker.QueryResponse.newBuilder().putMetadata("STATUS_OK", "").build();
    observer.onNext(Worker.ServerToBroker.newBuilder().setSubmitAck(okResponse).build());
    // 1 of 3 opchains reported
    observer.onNext(Worker.ServerToBroker.newBuilder().setOpchain(buildOpChainComplete(1, 0, 3)).build());
    // Server sends DONE early — 2 still owed
    observer.onNext(Worker.ServerToBroker.newBuilder().setDone(Worker.ServerDone.getDefaultInstance()).build());

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "awaitCompletion must return after DONE drains remaining 2 counts");
    Assert.assertEquals(session.getOutstandingCount(), 0L);
  }

  /**
   * Partial reports + onError: the latch is drained by exactly remaining-expected, not the full per-server count
   * (so we don't double-decrement).
   */
  @Test
  public void testErrorAfterPartialReportsDrainsRemainingOnly()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 5);
    StreamingDispatchObserver observer = new StreamingDispatchObserver(mockServer(), session, 5,
        (resp, err) -> { });
    StreamObserver<Worker.BrokerToServer> outbound = Mockito.mock(StreamObserver.class);
    observer.attachOutboundStream(outbound);
    session.registerStream(observer);

    Worker.QueryResponse okResponse = Worker.QueryResponse.newBuilder().putMetadata("STATUS_OK", "").build();
    observer.onNext(Worker.ServerToBroker.newBuilder().setSubmitAck(okResponse).build());
    observer.onNext(Worker.ServerToBroker.newBuilder().setOpchain(buildOpChainComplete(1, 0, 1)).build());
    observer.onNext(Worker.ServerToBroker.newBuilder().setOpchain(buildOpChainComplete(1, 1, 1)).build());
    // 3 of 5 still owed; stream errors.
    observer.onError(new RuntimeException("died"));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS));
    Assert.assertEquals(session.getOutstandingCount(), 0L);
  }

  // ---- helpers ----

  private static QueryServerInstance mockServer() {
    QueryServerInstance s = Mockito.mock(QueryServerInstance.class);
    Mockito.when(s.toString()).thenReturn("test-server");
    return s;
  }

  private static Worker.OpChainComplete buildOpChainComplete(int stageId, int workerId, long emitted)
      throws IOException {
    StatMap<AggregateOperator.StatKey> stat = new StatMap<>(AggregateOperator.StatKey.class)
        .merge(AggregateOperator.StatKey.EMITTED_ROWS, emitted);
    Worker.StageStatsNode rootNode = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(MultiStageOperator.Type.AGGREGATE.getId())
        .setStatMap(serialize(stat))
        .build();
    return Worker.OpChainComplete.newBuilder()
        .setStageId(stageId)
        .setWorkerId(workerId)
        .setSuccess(true)
        .setStats(Worker.MultiStageStatsTree.newBuilder()
            .setCurrentStageId(stageId)
            .setCurrentStage(rootNode)
            .build())
        .build();
  }

  private static ByteString serialize(StatMap<?> statMap)
      throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos)) {
      statMap.serialize(output);
      output.flush();
      return ByteString.copyFrom(baos.toByteArray());
    }
  }
}
