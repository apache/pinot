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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker-side gRPC client {@code StreamObserver} for the {@code SubmitWithStream} bidi RPC. Routes inbound
 * {@link Worker.ServerToBroker} messages to either the {@code submit_ack} callback (first message) or the
 * {@link StreamingQuerySession} (subsequent {@code OpChainComplete}s), and adapts the inbound side of the stream
 * (the {@link StreamObserver} we use to send {@code BrokerToServer} messages) into a
 * {@link StreamingServerHandle} so the session can fan out cancel.
 *
 * <p>Lifecycle — created when the broker opens a {@code SubmitWithStream} call to one server, then registered with
 * the session via {@link StreamingQuerySession#registerStream}. Receives:
 * <ol>
 *   <li>Exactly one {@code submit_ack} (always the first server→broker message).</li>
 *   <li>Zero or more {@code OpChainComplete} messages (one per opchain that ran on this server).</li>
 *   <li>Exactly one {@code ServerDone} after the last opchain has reported.</li>
 * </ol>
 *
 * <p>On {@code onError} or unexpected message order, drains the latch by {@code remainingExpected} via the session
 * so {@link StreamingQuerySession#awaitCompletion} can still finalize.
 */
public class StreamingDispatchObserver
    implements StreamObserver<Worker.ServerToBroker>, StreamingServerHandle {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingDispatchObserver.class);

  private final QueryServerInstance _server;
  private final StreamingQuerySession _session;
  /// Receives the submit-ack response or a failure throwable. Called exactly once per call (either with response on
  /// the first ServerToBroker.submit_ack, or with an error if the stream breaks before the ack arrives).
  private final BiConsumer<Worker.QueryResponse, Throwable> _ackCallback;
  private final int _expectedOpChainsForThisServer;
  private final AtomicBoolean _ackReceived = new AtomicBoolean(false);

  /**
   * Counts how many opchains we've already drained from the session for this server, so an onError doesn't
   * double-drain after some opchains already reported successfully.
   *
   * <p>Accessed only from gRPC inbound callbacks ({@code onNext}, {@code onError}, {@code onCompleted}), which gRPC
   * serializes per stream — no additional synchronization is needed. {@link #cancel} is the one method on this class
   * that may be called from a different thread (via {@link StreamingQuerySession#fanOutCancel}), but it only touches
   * {@link #_outbound} (which is {@code volatile}) and never reads or writes this field.
   */
  private int _opChainsReportedForThisServer = 0;

  /**
   * The inbound side of the bidi stream — used to send {@code submit} (initial) and {@code cancel} (fan-out) from
   * the broker. Set once via {@link #attachOutboundStream} after the gRPC stub is asked to start the stream; lives
   * for the duration of the call.
   */
  private volatile StreamObserver<Worker.BrokerToServer> _outbound;

  public StreamingDispatchObserver(QueryServerInstance server, StreamingQuerySession session,
      int expectedOpChainsForThisServer, BiConsumer<Worker.QueryResponse, Throwable> ackCallback) {
    _server = server;
    _session = session;
    _expectedOpChainsForThisServer = expectedOpChainsForThisServer;
    _ackCallback = ackCallback;
  }

  public QueryServerInstance getServer() {
    return _server;
  }

  /** Wires the outbound side of the bidi stream once the gRPC stub returns it. */
  public void attachOutboundStream(StreamObserver<Worker.BrokerToServer> outbound) {
    _outbound = outbound;
  }

  /** Sends the initial {@code BrokerToServer.submit} message on the outbound side. */
  public void sendSubmit(Worker.QueryRequest request) {
    StreamObserver<Worker.BrokerToServer> outbound = _outbound;
    if (outbound == null) {
      throw new IllegalStateException("attachOutboundStream must be called before sendSubmit");
    }
    outbound.onNext(Worker.BrokerToServer.newBuilder().setSubmit(request).build());
  }

  @Override
  public void onNext(Worker.ServerToBroker message) {
    switch (message.getPayloadCase()) {
      case SUBMIT_ACK:
        if (_ackReceived.compareAndSet(false, true)) {
          _ackCallback.accept(message.getSubmitAck(), null);
        } else {
          LOGGER.warn("Ignoring duplicate submit_ack from {}", _server);
        }
        break;
      case OPCHAIN:
        _session.recordOpChainComplete(message.getOpchain());
        _opChainsReportedForThisServer++;
        break;
      case DONE:
        int remaining = Math.max(0, _expectedOpChainsForThisServer - _opChainsReportedForThisServer);
        if (remaining > 0) {
          // Server declared done without all expected opchains reported (e.g. after STATUS_ERROR ack).
          // Drain the latch so awaitCompletion unblocks promptly rather than waiting until query deadline.
          // Intentionally reuses the error path: if this server couldn't execute its opchains the whole
          // query is doomed, so canceling other servers is correct.
          _session.recordStreamError(this, null, remaining);
          // Advance the reported counter so a subsequent onError (stream reset after DONE) sees remaining==0
          // and does not double-drain the latch.
          _opChainsReportedForThisServer = _expectedOpChainsForThisServer;
        } else {
          _session.unregisterStream(this);
        }
        break;
      case PAYLOAD_NOT_SET:
      default:
        LOGGER.warn("Unexpected ServerToBroker payload {} from {}", message.getPayloadCase(), _server);
        break;
    }
  }

  @Override
  public void onError(@Nullable Throwable t) {
    if (t != null && Status.fromThrowable(t).getCode() == Status.Code.UNIMPLEMENTED) {
      // Stream-stats mode has no automatic fallback to the unary Submit path (by design). A server that does not
      // implement the SubmitWithStream RPC fails every MSE query while the flag is on, so surface why loudly: this
      // is the rolling-upgrade / rollback hazard for enabling stream stats on a mixed-version cluster.
      LOGGER.error("Server {} does not implement the SubmitWithStream RPC that stream-stats mode requires. Disable "
              + "stream stats (query option '{}' or broker config '{}') until every server is upgraded; there is no "
              + "automatic fallback, so this query fails.", _server,
          CommonConstants.Broker.Request.QueryOptionKey.STREAM_STATS, CommonConstants.Broker.CONFIG_OF_STREAM_STATS);
    }
    int remaining = Math.max(0, _expectedOpChainsForThisServer - _opChainsReportedForThisServer);
    _session.recordStreamError(this, t, remaining);
    if (_ackReceived.compareAndSet(false, true)) {
      // The submit_ack never arrived — surface the error to the ack callback so the dispatcher's submit-ack wait
      // doesn't hang.
      _ackCallback.accept(null, t);
    }
  }

  @Override
  public void onCompleted() {
    _session.unregisterStream(this);
  }

  // --- StreamingServerHandle -------------------------------------------------

  /**
   * Sends a cancel message on the outbound stream. May be called from any thread — in particular from the
   * {@link StreamingQuerySession#fanOutCancel} path, which runs on whichever thread first observes a peer error and
   * iterates {@code _openStreams}. Thread-safety is achieved solely via the {@code volatile} read of
   * {@link #_outbound}; this method must not access any other mutable state of this instance.
   */
  @Override
  public void cancel(long requestId) {
    StreamObserver<Worker.BrokerToServer> outbound = _outbound;
    if (outbound == null) {
      return;
    }
    try {
      outbound.onNext(Worker.BrokerToServer.newBuilder()
          .setCancel(Worker.CancelRequest.newBuilder().setRequestId(requestId).build())
          .build());
    } catch (Throwable t) {
      LOGGER.debug("Cancel send failed on already-closed stream to {}: {}", _server, t.getMessage());
    }
  }
}
