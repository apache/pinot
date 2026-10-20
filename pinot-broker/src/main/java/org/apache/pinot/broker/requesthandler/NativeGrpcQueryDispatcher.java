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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.broker.routing.NativeBrokerQueryAdmission;
import org.apache.pinot.broker.routing.NativeBrokerQueryAdmission.Admission;
import org.apache.pinot.broker.routing.NativeBrokerRoutingImage;
import org.apache.pinot.broker.transport.NativeDataTableDecoder;
import org.apache.pinot.broker.transport.NativeGrpcQueryClient;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.grpc.ServerGrpcRequestBuilder;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Dispatches one admitted OFFLINE query using exact captured routes and the ordinary Pinot DataTable reducer.
/// Every required destination must return one complete, successful non-streaming response and successful EOF.
/// Failure cancels the entire attempt. This infrastructure does not register a listener or authorize final HTTP
/// output: its caller must retain the same admission through serialization and guarded network delivery.
public final class NativeGrpcQueryDispatcher implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeGrpcQueryDispatcher.class);
  private final String _brokerId;
  private final NativeBrokerQueryAdmission _authority;
  private final Transport _transport;
  @Nullable
  private final NativeGrpcQueryClient _ownedClient;
  private final Limits _limits;
  private final BrokerReduceService _reducer;
  private final Semaphore _slots;
  private final Set<Attempt> _attempts = new HashSet<>();
  private final AtomicBoolean _reducerStopped = new AtomicBoolean();
  private boolean _closed;

  /// Bounds aggregate work, including cancelled receivers that have not yet exited.
  public record Limits(int maxConcurrentQueries, int maxFanout, long maxResponseBytes) {
    public Limits {
      checkArgument(maxConcurrentQueries > 0 && maxConcurrentQueries <= 128
          && maxFanout > 0 && maxFanout <= 128 && maxResponseBytes > 0
          && maxResponseBytes <= 256L * 1024 * 1024, "Invalid native query dispatch limits");
    }
  }

  @FunctionalInterface
  interface Transport {
    NativeGrpcQueryClient.Call submit(URI endpoint, Server.ServerRequest request, LongSupplier remainingMillis);
  }

  public NativeGrpcQueryDispatcher(PinotConfiguration config, String brokerId, NativeBrokerQueryAdmission authority,
      NativeGrpcQueryClient client, Limits limits) {
    this(config, brokerId, authority, client::submit, limits, client);
  }

  @VisibleForTesting
  NativeGrpcQueryDispatcher(PinotConfiguration config, String brokerId, NativeBrokerQueryAdmission authority,
      Transport transport, Limits limits) {
    this(config, brokerId, authority, transport, limits, null);
  }

  private NativeGrpcQueryDispatcher(PinotConfiguration config, String brokerId, NativeBrokerQueryAdmission authority,
      Transport transport, Limits limits, @Nullable NativeGrpcQueryClient ownedClient) {
    _brokerId = requireNonNull(brokerId);
    _authority = requireNonNull(authority);
    _transport = requireNonNull(transport);
    _ownedClient = ownedClient;
    _limits = requireNonNull(limits);
    _slots = new Semaphore(limits.maxConcurrentQueries());
    _reducer = new BrokerReduceService(config);
  }

  /// The caller owns the normal QueryThreadContext and the original admission for the whole request lifetime.
  public BrokerResponseNative dispatch(long requestId, String correlationId, BrokerRequest original,
      BrokerRequest server, TableRouteInfo route, Admission admission, boolean trace) throws Exception {
    Attempt attempt = begin();
    try {
      _authority.checkQueryDelivery(admission);
      validateOptions(original);
      List<Target> targets = requests(requestId, correlationId, server, route, admission, trace);
      AtomicLong receivedBytes = new AtomicLong();
      BlockingQueue<Received> completed = new ArrayBlockingQueue<>(targets.size());
      for (Target target : targets) {
        attempt.checkOpen();
        _authority.checkQueryDelivery(admission);
        NativeGrpcQueryClient.Call call =
            _transport.submit(target.endpoint(), target.request(), () -> _authority.remainingMillis(admission));
        attempt.add(call);
        attempt.receive(() -> {
          try {
            DataTable table = receive(requestId, target, call, admission, attempt, receivedBytes);
            _authority.deliverQuery(admission, () -> {
              attempt.checkOpen();
              completed.add(new Received(target.server(), table, null));
            });
          } catch (Exception e) {
            completed.add(new Received(target.server(), null, e));
            attempt.cancel();
          }
        });
      }
      Map<ServerRoutingInstance, DataTable> tables = new HashMap<>();
      for (int i = 0; i < targets.size(); i++) {
        // Anchor before reading the qualified budget so a pause cannot restart the receive timeout.
        long anchor = System.nanoTime();
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(_authority.remainingMillis(admission));
        long waitNanos = remainingNanos - Math.max(0, System.nanoTime() - anchor);
        Received received = completed.poll(Math.max(0, waitNanos), TimeUnit.NANOSECONDS);
        if (received == null) {
          throw new TimeoutException("Native query fanout did not complete within its original deadline");
        }
        if (received.failure() != null) {
          throw received.failure();
        }
        _authority.deliverQuery(admission, () -> {
          attempt.checkOpen();
          checkState(tables.put(received.server(), received.table()) == null, "Repeated native server result");
        });
      }
      attempt.checkOpen();
      long remaining = _authority.remainingMillis(admission);
      BrokerResponseNative result;
      // Keep reduction, including group-by subtasks, on this attempt's stack. The legacy reducer's asynchronous
      // timeout/cancel does not wait for worker exit and cannot determine when native query capacity is reusable.
      try (var executor = MoreExecutors.newDirectExecutorService()) {
        result = _reducer.reduceOnDataTable(original, server, tables, remaining, BrokerMetrics.get(), executor);
      }
      checkState(!result.isPartialResult(), "Native query reduction did not produce a complete result");
      result.setNumServersQueried(targets.size());
      result.setNumServersResponded(targets.size());
      _authority.deliverQuery(admission, attempt::checkOpen);
      return result;
    } finally {
      attempt.finish();
    }
  }

  private List<Target> requests(long requestId, String correlationId, BrokerRequest server, TableRouteInfo route,
      Admission admission, boolean trace) {
    NativeBrokerRoutingImage image = requireNonNull(admission.image());
    checkArgument(image.tableName().equals(route.getOfflineTableName()) && route.getRealtimeTableName() == null
        && route.getRealtimeBrokerRequest() == null && route.getRealtimeRoutingTable() == null
        && server.equals(route.getOfflineBrokerRequest()) && route.getUnavailableSegments().isEmpty()
        && !route.isDisabled() && route.getNumPrunedSegmentsTotal() == 0
        && image.tableName().equals(server.getPinotQuery().getDataSource().getTableName()),
        "Native dispatch requires the complete captured physical OFFLINE route");
    validateOptions(server);
    List<NativeBrokerRoutingImage.ServerRequest> admitted = List.copyOf(admission.requests());
    checkArgument(!admitted.isEmpty() && admitted.size() <= _limits.maxFanout(), "Invalid native query fanout");
    var first = admitted.getFirst().context();
    List<NativeBrokerRoutingImage.ServerRequest> expected =
        image.requests(first.queryId(), first.attemptId(), requireNonNull(first.deadline()));
    checkArgument(expected.size() == admitted.size(), "Native admission has incomplete segment coverage");
    Map<ServerInstance, SegmentsToQuery> routes = requireNonNull(route.getOfflineRoutingTable());
    checkArgument(routes.size() == expected.size(), "Native route dropped or added a destination");
    Map<String, Map.Entry<ServerInstance, SegmentsToQuery>> byId = new HashMap<>();
    routes.entrySet().forEach(entry -> checkArgument(
        byId.put(entry.getKey().getInstanceId(), entry) == null, "Repeated native routing server"));
    List<Target> targets = new ArrayList<>();
    for (int i = 0; i < expected.size(); i++) {
      var target = expected.get(i);
      var context = target.context();
      checkArgument(target.endpoint().equals(admitted.get(i).endpoint())
          && context.toProto().equals(admitted.get(i).context().toProto())
          && context.broker().participantId().equals(_brokerId), "Native identity envelope differs from admission");
      var entry = requireNonNull(byId.remove(context.server().participantId()), "Missing admitted destination");
      ServerInstance instance = entry.getKey();
      SegmentsToQuery segments = entry.getValue();
      checkArgument(target.endpoint().getHost().equals(instance.getHostname())
          && target.endpoint().getPort() == instance.getGrpcPort()
          && segments.getOptionalSegments().isEmpty()
          && segments.getSegments().size() == context.requiredNames().size()
          && new HashSet<>(segments.getSegments()).equals(new HashSet<>(context.requiredNames())),
          "Native routing endpoint or segment coverage differs from admission");
      Server.ServerRequest wire = new ServerGrpcRequestBuilder().setRequestId(requestId).setCid(correlationId)
          .setBrokerId(_brokerId).setEnableTrace(trace).setEnableStreaming(false).setBrokerRequest(server)
          .setSegments(context.requiredNames()).build().toBuilder().setNativeQueryContext(context.toProto()).build();
      targets.add(new Target(target.endpoint(),
          new ServerRoutingInstance(instance.getInstanceId(), instance.getHostname(), instance.getGrpcPort(),
              TableType.OFFLINE, true), wire));
    }
    return List.copyOf(targets);
  }

  private static void validateOptions(BrokerRequest request) {
    Map<String, String> options = request.getPinotQuery().getQueryOptions();
    checkArgument(options == null || !QueryOptionsUtils.isIgnoreMissingSegments(options),
        "Native dispatch cannot ignore required missing segments");
    checkArgument(QueryOptionsUtils.getTableSampler(options) == null,
        "Native dispatch cannot sample required segment coverage");
  }

  private DataTable receive(long requestId, Target target, NativeGrpcQueryClient.Call call, Admission admission,
      Attempt attempt, AtomicLong receivedBytes) throws IOException {
    Iterator<Server.ServerResponse> responses = call.responses();
    checkReceiving(admission, attempt);
    checkState(responses.hasNext(), "Native server returned no DataTable");
    checkReceiving(admission, attempt);
    Server.ServerResponse response = responses.next();
    checkReceiving(admission, attempt);
    checkState(Response.ResponseType.NON_STREAMING.equals(
        response.getMetadataOrDefault(Response.MetadataKeys.RESPONSE_TYPE, "")),
        "Native server returned an unexpected response type");
    checkState(receivedBytes.addAndGet(response.getSerializedSize()) <= _limits.maxResponseBytes(),
        "Native query response exceeds its aggregate byte bound");
    DataTable table = NativeDataTableDecoder.decode(response.getPayload().asReadOnlyByteBuffer());
    checkReceiving(admission, attempt);
    checkState(table.getExceptions().isEmpty() && table.getDataSchema() != null
        && Long.toString(requestId).equals(table.getMetadata().get(DataTable.MetadataKey.REQUEST_ID.getName()))
        && Integer.toString(target.request().getSegmentsCount()).equals(
            table.getMetadata().get(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName()))
        && !Boolean.parseBoolean(table.getMetadata().get(DataTable.MetadataKey.INCOMPLETE_MERGE.getName())),
        "Native server result is incomplete or does not match the required request");
    checkState(!responses.hasNext(), "Native server returned multiple DataTables");
    checkReceiving(admission, attempt);
    return table;
  }

  private void checkReceiving(Admission admission, Attempt attempt) {
    attempt.checkOpen();
    _authority.checkQueryDelivery(admission);
  }

  private synchronized Attempt begin() {
    checkState(!_closed, "Native dispatcher is closed");
    checkState(_slots.tryAcquire(), "Native query dispatch capacity exhausted");
    Attempt attempt = new Attempt();
    _attempts.add(attempt);
    return attempt;
  }

  private void release(Attempt attempt) {
    boolean stop;
    synchronized (this) {
      checkState(_attempts.remove(attempt), "Native query attempt was already released");
      _slots.release();
      stop = _closed && _attempts.isEmpty();
    }
    if (stop) {
      stopReducer();
    }
  }

  @Override
  public void close() {
    List<Attempt> attempts;
    synchronized (this) {
      _closed = true;
      attempts = List.copyOf(_attempts);
    }
    attempts.forEach(Attempt::cancel);
    if (_ownedClient != null) {
      _ownedClient.close();
    }
    if (attempts.isEmpty()) {
      stopReducer();
    }
  }

  private void stopReducer() {
    if (_reducerStopped.compareAndSet(false, true)) {
      _reducer.shutDown();
    }
  }

  private record Target(URI endpoint, ServerRoutingInstance server, Server.ServerRequest request) {
  }

  private record Received(ServerRoutingInstance server, @Nullable DataTable table, @Nullable Exception failure) {
  }

  /// Keeps query capacity until both dispatch and every receiver have exited, even after cancellation.
  private final class Attempt {
    private final List<NativeGrpcQueryClient.Call> _calls = new ArrayList<>();
    private final Set<Thread> _receivers = new HashSet<>();
    private volatile boolean _cancelled;
    private boolean _finished;

    private void checkOpen() {
      if (_cancelled) {
        throw new CancellationException("Native query attempt cancelled");
      }
    }

    private synchronized void add(NativeGrpcQueryClient.Call call) {
      _calls.add(call);
      if (_cancelled) {
        call.close();
        checkOpen();
      }
    }

    private synchronized void receive(Runnable receive) {
      checkOpen();
      Thread thread = Thread.ofVirtual().name("native-query-receiver").unstarted(() -> {
        try {
          checkOpen();
          receive.run();
        } finally {
          boolean release;
          synchronized (this) {
            _receivers.remove(Thread.currentThread());
            release = _finished && _receivers.isEmpty();
          }
          if (release) {
            NativeGrpcQueryDispatcher.this.release(this);
          }
        }
      });
      _receivers.add(thread);
      try {
        thread.start();
      } catch (RuntimeException | Error e) {
        _receivers.remove(thread);
        throw e;
      }
    }

    private synchronized void cancel() {
      _cancelled = true;
      for (NativeGrpcQueryClient.Call call : _calls) {
        try {
          call.close();
        } catch (RuntimeException e) {
          LOGGER.warn("Could not close a cancelled native query call", e);
        }
      }
      _receivers.forEach(Thread::interrupt);
    }

    private void finish() {
      cancel();
      boolean release;
      synchronized (this) {
        _finished = true;
        release = _receivers.isEmpty();
      }
      if (release) {
        NativeGrpcQueryDispatcher.this.release(this);
      }
    }
  }
}
