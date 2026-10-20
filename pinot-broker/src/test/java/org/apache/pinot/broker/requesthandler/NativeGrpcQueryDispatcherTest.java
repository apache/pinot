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

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.pinot.broker.routing.NativeBrokerQueryAdmission;
import org.apache.pinot.broker.routing.NativeBrokerQueryAdmission.Admission;
import org.apache.pinot.broker.routing.NativeBrokerRoutingImage;
import org.apache.pinot.broker.transport.NativeGrpcQueryClient;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.ResultsBlockUtils;
import org.apache.pinot.core.operator.streaming.StreamingResponseUtils;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteProvider;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises the ordinary reducer with real DataTables and exact native fanout, including failures after partial work.
public class NativeGrpcQueryDispatcherTest {
  private static final long REQUEST_ID = 42;
  private static final String BROKER_ID = "Broker_native";
  private static final String TABLE = "orders_OFFLINE";
  private static final String SELECTION = "SELECT id FROM orders_OFFLINE ORDER BY id DESC LIMIT 3";

  @Test
  public void testTwoServerSelectionAndAggregationUseNormalReducer() throws Exception {
    // Server selection results are already sorted in query order for the broker's normal N-way merge.
    try (Fixture fixture = new Fixture(List.of(selection(4, 1)), List.of(selection(3, 2)))) {
      BrokerResponseNative result = fixture.dispatch(SELECTION);
      assertEquals(result.getResultTable().getRows().stream().map(row -> row[0]).toList(), List.of(4, 3, 2));
      assertTrue(result.getExceptions().isEmpty());
      assertEquals(result.getNumSegmentsQueried(), 2L);
      assertEquals(result.getNumServersQueried(), 2);
      assertEquals(result.getNumServersResponded(), 2);
      fixture.assertRequests();
      fixture.assertClosed();
    }
    try (Fixture fixture = new Fixture(List.of(aggregate(5, 2)), List.of(aggregate(5, 2)))) {
      BrokerResponseNative result = fixture.dispatch("SELECT SUM(id), COUNT(*) FROM orders_OFFLINE");
      assertEquals(result.getResultTable().getRows().size(), 1);
      assertEquals(result.getResultTable().getRows().getFirst(), new Object[]{10D, 4L});
      assertTrue(result.getExceptions().isEmpty());
      assertEquals(result.getNumSegmentsQueried(), 2L);
      fixture.assertClosed();
    }
    try (Fixture fixture = new Fixture(List.of(groupedSum(1, 2)), List.of(groupedSum(1, 3)))) {
      BrokerResponseNative result =
          fixture.dispatch("SELECT id, SUM(id) FROM orders_OFFLINE GROUP BY id ORDER BY id");
      assertEquals(result.getResultTable().getRows().size(), 3);
      assertEquals(result.getResultTable().getRows().get(0), new Object[]{1, 2D});
      assertEquals(result.getResultTable().getRows().get(1), new Object[]{2, 2D});
      assertEquals(result.getResultTable().getRows().get(2), new Object[]{3, 3D});
      assertTrue(result.getExceptions().isEmpty());
      assertEquals(result.getNumSegmentsQueried(), 2L);
      fixture.assertClosed();
    }
    var query = QueryContextConverterUtils.getQueryContext(
        CalciteSqlCompiler.compileToBrokerRequest(SELECTION).getPinotQuery());
    // The real all-pruned server path returns an empty STRING schema, even when another server returns INT rows.
    DataTable empty = metadata(ResultsBlockUtils.buildEmptyQueryResults(query).getDataTable(), 0);
    assertEquals(empty.getDataSchema().getColumnDataType(0), ColumnDataType.STRING);
    try (Fixture fixture = new Fixture(
        List.of(StreamingResponseUtils.getNonStreamingResponse(empty)), List.of(selection(2)))) {
      BrokerResponseNative result = fixture.dispatch(SELECTION);
      assertEquals(result.getResultTable().getRows().stream().map(row -> row[0]).toList(), List.of(2));
      assertTrue(result.getExceptions().isEmpty());
      assertEquals(result.getNumSegmentsQueried(), 2L);
      fixture.assertClosed();
    }
  }

  @Test(timeOut = 10_000)
  public void testSubmissionAndReceiveFailureCancelEveryStartedCall() throws Exception {
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)))) {
      fixture._failSubmission = 2;
      assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
      assertEquals(fixture._submissions.get(), 2);
      assertTrue(fixture._calls.getFirst()._cancelled.get(), "A failed second send must cancel the first RPC");
      assertTrue(fixture._calls.getFirst()._closed.get());
    }
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)));
        var executor = Executors.newSingleThreadExecutor()) {
      CountDownLatch blocked = new CountDownLatch(1);
      ControlledCall first = fixture._calls.getFirst();
      ControlledCall second = fixture._calls.get(1);
      first._beforeRead = () -> {
        blocked.countDown();
        await(first._cancelledSignal);
        throw new CancellationException("Cancelled blocked peer");
      };
      second._beforeRead = () -> {
        await(blocked);
        throw Status.UNAVAILABLE.withDescription("Server response failed").asRuntimeException();
      };
      var dispatched = executor.submit(() -> {
        assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
        return null;
      });
      try {
        assertTrue(blocked.await(1, TimeUnit.SECONDS));
        dispatched.get(2, TimeUnit.SECONDS);
        fixture.assertCancelled();
      } finally {
        fixture._calls.forEach(ControlledCall::cancel);
      }
    }
  }

  @Test
  public void testCompleteSingleResponseAndExactCoverageAreRequired() throws Exception {
    Server.ServerResponse valid = selection(1);
    DataTable missing = selectionTable(2);
    missing.getMetadata().put(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName(), "0");
    DataTable failed = selectionTable(2);
    failed.addException(QueryErrorCode.SERVER_SEGMENT_MISSING, "Required segment disappeared");
    DataTable wrongRequest = selectionTable(2);
    wrongRequest.getMetadata().put(DataTable.MetadataKey.REQUEST_ID.getName(), "41");
    List<List<Server.ServerResponse>> malformed = List.of(
        List.of(),
        List.of(valid, valid),
        List.of(valid.toBuilder().putMetadata(Response.MetadataKeys.RESPONSE_TYPE, Response.ResponseType.DATA).build()),
        List.of(StreamingResponseUtils.getNonStreamingResponse(missing)),
        List.of(StreamingResponseUtils.getNonStreamingResponse(failed)),
        List.of(StreamingResponseUtils.getNonStreamingResponse(wrongRequest)),
        List.of(StreamingResponseUtils.getNonStreamingResponse(
            metadata(DataTableBuilderFactory.getEmptyDataTable(), 0))));
    for (List<Server.ServerResponse> responses : malformed) {
      try (Fixture fixture = new Fixture(List.of(valid), responses)) {
        assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
        fixture.assertCancelled();
      }
    }
    try (Fixture fixture = new Fixture(List.of(valid), List.of(valid))) {
      fixture._calls.get(1)._onEnd = () -> {
        throw Status.UNAVAILABLE.withDescription("Failure after the full DataTable").asRuntimeException();
      };
      assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
      fixture.assertCancelled();
    }
  }

  @Test
  public void testRevocationDuringReadAndCompletionCannotExposeSuccess() throws Exception {
    for (boolean revokeAtEnd : List.of(false, true)) {
      try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)))) {
        if (revokeAtEnd) {
          fixture._calls.get(1)._onEnd = () -> fixture._revoked.set(true);
        } else {
          fixture._calls.get(1)._onNext = () -> fixture._revoked.set(true);
        }
        assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
        fixture.assertCancelled();
      }
    }
  }

  @Test
  public void testRouteOrEnvelopeCannotDropAdmittedSegments() throws Exception {
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)))) {
      BrokerRequest request = CalciteSqlCompiler.compileToBrokerRequest(SELECTION);
      ImplicitHybridTableRouteInfo route = (ImplicitHybridTableRouteInfo) fixture.route(request);
      Map<?, ?> original = route.getOfflineRoutingTable();
      var reduced = new LinkedHashMap<>(route.getOfflineRoutingTable());
      reduced.remove(reduced.keySet().iterator().next());
      route.setOfflineRoutingTable(reduced);
      assertEquals(original.size(), 2);
      try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
        assertThrows(Exception.class, () -> fixture._dispatcher.dispatch(REQUEST_ID, "query-42", request, request,
            route, fixture._admission, false));
      }
      assertEquals(fixture._submissions.get(), 0);
    }
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)))) {
      when(fixture._admission.requests()).thenReturn(List.of(fixture._requests.getFirst()));
      assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
      assertEquals(fixture._submissions.get(), 0);
    }
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)))) {
      BrokerRequest request = CalciteSqlCompiler.compileToBrokerRequest(SELECTION);
      request.getPinotQuery().putToQueryOptions(QueryOptionKey.IGNORE_MISSING_SEGMENTS, "true");
      assertThrows(Exception.class, () -> fixture.dispatch(request));
      assertEquals(fixture._submissions.get(), 0, "Required coverage cannot be weakened by a legacy query option");
    }
  }

  @Test(timeOut = 10_000)
  public void testCancelledReceiverRetainsCapacityAndAggregateResponseBytesAreBounded() throws Exception {
    try (Fixture fixture = new Fixture(List.of(selection(1)), List.of(selection(2)),
        new NativeGrpcQueryDispatcher.Limits(1, 4, 1024 * 1024));
        var executor = Executors.newSingleThreadExecutor()) {
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicReference<Thread> receiver = new AtomicReference<>();
      fixture._calls.getFirst()._beforeRead = () -> {
        receiver.set(Thread.currentThread());
        entered.countDown();
        boolean interrupted = false;
        try {
          // Cancellation has reached the transport, but its receiver still owns resources until this is released.
          while (release.getCount() != 0) {
            try {
              release.await();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
        throw new CancellationException("Slow receiver has finally stopped");
      };
      fixture._calls.get(1)._beforeRead = () -> {
        await(entered);
        throw Status.UNAVAILABLE.withDescription("Other receiver failed").asRuntimeException();
      };
      var dispatched = executor.submit(() -> {
        assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
        return null;
      });
      try {
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        dispatched.get(2, TimeUnit.SECONDS);
        fixture.assertCancelled();
        assertTrue(receiver.get().isAlive(), "The cancelled receiver deliberately still holds resources");
        IllegalStateException full = expectThrows(IllegalStateException.class, () -> fixture.dispatch(SELECTION));
        assertTrue(full.getMessage().contains("capacity"), "Cancellation must not prematurely free query capacity");
        assertEquals(fixture._submissions.get(), 2);
        fixture._dispatcher.close();
        IllegalStateException closed = expectThrows(IllegalStateException.class, () -> fixture.dispatch(SELECTION));
        assertTrue(closed.getMessage().contains("closed"));
        assertEquals(fixture._submissions.get(), 2, "Closing must prevent any new transport submission");
      } finally {
        release.countDown();
        Thread thread = receiver.get();
        if (thread != null) {
          thread.join(2000);
          assertFalse(thread.isAlive(), "Release must allow the cancelled receiver to finish cleanup");
        }
      }
      assertThrows(IllegalStateException.class, () -> fixture.dispatch(SELECTION));
    }
    Server.ServerResponse first = selection(1);
    Server.ServerResponse second = selection(2);
    long combinedLimit = first.getSerializedSize() + second.getSerializedSize() - 1L;
    try (Fixture fixture = new Fixture(List.of(first), List.of(second),
        new NativeGrpcQueryDispatcher.Limits(1, 4, combinedLimit))) {
      // Each complete server response fits alone; admitting both must still enforce the aggregate query limit.
      assertTrue(first.getSerializedSize() <= combinedLimit && second.getSerializedSize() <= combinedLimit);
      assertThrows(Exception.class, () -> fixture.dispatch(SELECTION));
      assertEquals(fixture._submissions.get(), 2);
      fixture.assertCancelled();
    }
  }

  private static Server.ServerResponse selection(int... values) throws IOException {
    return StreamingResponseUtils.getNonStreamingResponse(selectionTable(values));
  }

  private static DataTable selectionTable(int... values) throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(
        new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.INT}));
    for (int value : values) {
      builder.startRow();
      builder.setColumn(0, value);
      builder.finishRow();
    }
    return metadata(builder.build(), values.length);
  }

  private static Server.ServerResponse aggregate(double sum, long count) throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(new DataSchema(
        new String[]{"sum(id)", "count(*)"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG}));
    builder.startRow();
    builder.setColumn(0, sum);
    builder.setColumn(1, count);
    builder.finishRow();
    return StreamingResponseUtils.getNonStreamingResponse(metadata(builder.build(), count));
  }

  private static Server.ServerResponse groupedSum(int... ids) throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(
        new DataSchema(new String[]{"id", "sum(id)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE}));
    for (int id : ids) {
      builder.startRow();
      builder.setColumn(0, id);
      builder.setColumn(1, (double) id);
      builder.finishRow();
    }
    return StreamingResponseUtils.getNonStreamingResponse(metadata(builder.build(), ids.length));
  }

  private static DataTable metadata(DataTable table, long docs) {
    table.getMetadata().put(DataTable.MetadataKey.REQUEST_ID.getName(), Long.toString(REQUEST_ID));
    table.getMetadata().put(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName(), "1");
    table.getMetadata().put(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), "1");
    table.getMetadata().put(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName(), "1");
    table.getMetadata().put(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName(), Long.toString(docs));
    return table;
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(3, TimeUnit.SECONDS), "Expected concurrent dispatch/cancellation did not arrive");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static final class ControlledCall implements NativeGrpcQueryClient.Call {
    private final List<Server.ServerResponse> _frames;
    private final AtomicBoolean _cancelled = new AtomicBoolean();
    private final AtomicBoolean _closed = new AtomicBoolean();
    private final CountDownLatch _cancelledSignal = new CountDownLatch(1);
    private Runnable _beforeRead = () -> { };
    private Runnable _onNext = () -> { };
    private Runnable _onEnd = () -> { };

    private ControlledCall(List<Server.ServerResponse> frames) {
      _frames = List.copyOf(frames);
    }

    @Override
    public Iterator<Server.ServerResponse> responses() {
      return new Iterator<>() {
        private int _index;

        @Override
        public boolean hasNext() {
          if (_cancelled.get()) {
            throw new CancellationException("RPC cancelled");
          }
          _beforeRead.run();
          if (_index == _frames.size()) {
            _onEnd.run();
            return false;
          }
          return true;
        }

        @Override
        public Server.ServerResponse next() {
          _onNext.run();
          return _frames.get(_index++);
        }
      };
    }

    @Override
    public void cancel() {
      _cancelled.set(true);
      _cancelledSignal.countDown();
    }

    @Override
    public void close() {
      _closed.set(true);
      cancel();
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final NativeBrokerQueryAdmission _authority = mock(NativeBrokerQueryAdmission.class);
    private final Admission _admission = mock(Admission.class);
    private final AtomicBoolean _revoked = new AtomicBoolean();
    private final AtomicInteger _submissions = new AtomicInteger();
    private final List<ControlledCall> _calls;
    private final List<Server.ServerRequest> _sent = new ArrayList<>();
    private final NativeBrokerRoutingImage _image = image();
    private final List<NativeBrokerRoutingImage.ServerRequest> _requests = _image.requests(
        UUID.randomUUID(), UUID.randomUUID(), new NativeQueryDeadline("test-clock", 1000, 11000));
    private final NativeGrpcQueryDispatcher _dispatcher;
    private int _failSubmission;

    private Fixture(List<Server.ServerResponse> first, List<Server.ServerResponse> second) throws IOException {
      this(first, second, new NativeGrpcQueryDispatcher.Limits(2, 4, 1024 * 1024));
    }

    private Fixture(List<Server.ServerResponse> first, List<Server.ServerResponse> second,
        NativeGrpcQueryDispatcher.Limits limits) throws IOException {
      _calls = List.of(new ControlledCall(first), new ControlledCall(second));
      when(_admission.image()).thenReturn(_image);
      when(_admission.requests()).thenReturn(_requests);
      when(_admission.planningMetadata(false)).thenAnswer(ignored -> _image.planningMetadata(false));
      doAnswer(ignored -> {
        checkAuthority();
        return null;
      }).when(_authority).checkQueryDelivery(_admission);
      doAnswer(invocation -> {
        checkAuthority();
        invocation.<Runnable>getArgument(1).run();
        return null;
      }).when(_authority).deliverQuery(any(), any());
      when(_authority.remainingMillis(_admission)).thenAnswer(ignored -> {
        checkAuthority();
        return 10_000L;
      });
      _dispatcher = new NativeGrpcQueryDispatcher(new PinotConfiguration(), BROKER_ID, _authority, this::submit,
          limits);
    }

    private NativeGrpcQueryClient.Call submit(URI endpoint, Server.ServerRequest request, LongSupplier remaining) {
      assertTrue(remaining.getAsLong() > 0);
      int submission = _submissions.incrementAndGet();
      if (submission == _failSubmission) {
        throw Status.UNAVAILABLE.withDescription("Connection failed before submission").asRuntimeException();
      }
      int index = _requests.stream().map(NativeBrokerRoutingImage.ServerRequest::endpoint).toList().indexOf(endpoint);
      assertTrue(index >= 0, "Dispatch must use the captured approved endpoint");
      _sent.add(request);
      return _calls.get(index);
    }

    private void checkAuthority() {
      if (_revoked.get()) {
        throw new IllegalStateException("Captured broker authority revoked");
      }
    }

    private BrokerResponseNative dispatch(String sql) throws Exception {
      return dispatch(CalciteSqlCompiler.compileToBrokerRequest(sql));
    }

    private BrokerResponseNative dispatch(BrokerRequest request) throws Exception {
      try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
        return _dispatcher.dispatch(REQUEST_ID, "query-42", request, request, route(request), _admission, false);
      }
    }

    private TableRouteInfo route(BrokerRequest request) {
      var planning = _image.planningMetadata(false);
      var provider = new ImplicitHybridTableRouteProvider();
      var route = provider.getTableRouteInfo(TABLE, planning.tableCache(), planning.routingManager());
      provider.calculateRoutes(route, planning.routingManager(), request, null, REQUEST_ID);
      return route;
    }

    private void assertRequests() {
      assertEquals(_sent.size(), 2);
      for (Server.ServerRequest request : _sent) {
        NativeQueryContext context = NativeQueryContext.fromProto(request.getNativeQueryContext());
        var expected = _requests.stream().filter(target ->
            target.context().server().equals(context.server())).findFirst().orElseThrow();
        assertEquals(context.toProto(), expected.context().toProto());
        assertEquals(request.getSegmentsList(), expected.context().requiredNames());
        assertEquals(request.getMetadataOrThrow(Request.MetadataKeys.ENABLE_STREAMING), "false");
        assertEquals(request.getMetadataOrThrow(Request.MetadataKeys.REQUEST_ID), Long.toString(REQUEST_ID));
        assertEquals(request.getMetadataOrThrow(Request.MetadataKeys.CORRELATION_ID), "query-42");
        assertEquals(request.getMetadataOrThrow(Request.MetadataKeys.BROKER_ID), BROKER_ID);
        assertFalse(request.getPayload().isEmpty(), "Use the compiled ordinary BrokerRequest");
      }
    }

    private void assertClosed() {
      _calls.forEach(call -> assertTrue(call._closed.get(), "Each started RPC must be closed"));
    }

    private void assertCancelled() {
      _calls.forEach(call -> assertTrue(call._cancelled.get(), "A failed query must cancel every started RPC"));
      assertClosed();
    }

    @Override
    public void close() {
      _dispatcher.close();
    }
  }

  private static NativeBrokerRoutingImage image() throws IOException {
    String history = UUID.randomUUID().toString();
    var broker = token(history, BROKER_ID, MetadataRpc.ParticipantRole.BROKER);
    var table = object(history);
    var schema = object(history);
    var revision = MetadataRpc.Revision.newBuilder().setHistoryId(history).setIndex(20);
    var applied = MetadataRpc.Revision.newBuilder().setHistoryId(history).setIndex(1);
    var control = MetadataRpc.MembershipControl.newBuilder().setControlEpoch(MetadataRpc.ControlToken.newBuilder()
            .setHistoryId(history).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
            .setActivationId(UUID.randomUUID().toString())).setClockBarrierIndex(1).setMinimumTickDelayMillis(100)
        .setMaxConfiguredPermitLifetimeMillis(200).setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1)
            .setMinTickIntervalMillis(100).setLeaseRounds(3).setPermitLifetimeMillis(200)
            .setMaxQueryLifetimeMillis(300).setMaxRevisionLag(100));
    var tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
    var schemaConfig = new Schema.SchemaBuilder().setSchemaName("orders")
        .addSingleValueDimension("id", DataType.INT).build();
    var routing = MetadataRpc.TableRouting.newBuilder().setTableName(TABLE).setTable(table)
        .setSchemaName("orders").setSchema(schema)
        .setTableConfig(record(table, TABLE, MetadataRpc.RecordKind.TABLE_CONFIG,
            MetadataRpc.CatalogValue.newBuilder().setTableConfigJson(
                ByteString.copyFrom(JsonUtils.objectToBytes(tableConfig))).build()))
        .setSchemaRecord(record(schema, "orders", MetadataRpc.RecordKind.SCHEMA,
            MetadataRpc.CatalogValue.newBuilder().setSchemaJson(
                ByteString.copyFrom(JsonUtils.objectToBytes(schemaConfig))).build()));
    for (int i = 0; i < 2; i++) {
      String endpoint = "grpcs://server-" + i + ":8090";
      var server = token(history, "Server_" + i, MetadataRpc.ParticipantRole.SERVER);
      routing.addSegments(MetadataRpc.SegmentRouting.newBuilder().setName("segment-" + i)
          .setContent(object(history)).setContentSha256("a".repeat(64))
          .addReplicas(MetadataRpc.ReplicaRouting.newBuilder().setServer(server).setAssignment(object(history))
              .setAssignmentEpoch(1).setMaterializationId(UUID.randomUUID().toString())
              .setInstallationId(UUID.randomUUID().toString()).setEndpoint(endpoint)));
      routing.addServerAuthorities(MetadataRpc.ParticipantState.newBuilder().setParticipant(server)
          .setReady(true).setExpiryRound(3).setLastModifiedIndex(2).setAppliedRevision(applied)
          .addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY)
          .putEndpoints("queryGrpc", endpoint).putEndpoints("nativeQueryVersion", "2"));
    }
    var snapshot = MetadataRpc.RoutingSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.OK)
        .setRevision(revision).setControl(control).setTable(routing)
        .setParticipant(MetadataRpc.ParticipantState.newBuilder().setParticipant(broker).setReady(true)
            .setExpiryRound(3).setLastModifiedIndex(2).setAppliedRevision(applied)
            .addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY)).build();
    return new NativeBrokerRoutingImage(snapshot, broker, TABLE);
  }

  private static MetadataRpc.ParticipantToken token(String history, String id, MetadataRpc.ParticipantRole role) {
    return MetadataRpc.ParticipantToken.newBuilder().setHistoryId(history).setParticipantId(id).setRole(role)
        .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1).build();
  }

  private static MetadataRpc.ObjectVersion object(String history) {
    return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(history).setObjectId(UUID.randomUUID().toString())
        .setVersion(1).build();
  }

  private static MetadataRpc.CatalogRecord record(MetadataRpc.ObjectVersion identity, String name,
      MetadataRpc.RecordKind kind, MetadataRpc.CatalogValue value) {
    return MetadataRpc.CatalogRecord.newBuilder().setKind(kind).setName(name).setObjectId(identity.getObjectId())
        .setVersion(1).setSchemaVersion(1).setLastModifiedIndex(1).setValue(value).build();
  }
}
