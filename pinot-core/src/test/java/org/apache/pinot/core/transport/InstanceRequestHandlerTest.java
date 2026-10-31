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
package org.apache.pinot.core.transport;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InstanceRequestHandlerTest {

  @BeforeClass
  public void setUp() {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    ServerMetrics.register(new ServerMetrics(registry));
  }

  /// A ping is answered straight from the network thread: the scheduler is never touched, so a server whose queries
  /// are queued, stuck or slow still answers it at once.
  @Test
  public void testPingIsAnsweredWithoutTouchingTheScheduler()
      throws Exception {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    EmbeddedChannel channel = new EmbeddedChannel(
        new InstanceRequestHandler("server01", new PinotConfiguration(), queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant()));

    long requestId = -42L;
    channel.writeInbound(Unpooled.wrappedBuffer(serialize(QueryRouter.newPingRequest(requestId, "broker01"))));

    ByteBuf reply = channel.readOutbound();
    byte[] replyBytes = new byte[reply.readableBytes()];
    reply.readBytes(replyBytes);
    reply.release();
    DataTable dataTable = DataTableFactory.getDataTable(replyBytes);
    assertEquals(dataTable.getMetadata().get(DataTable.MetadataKey.REQUEST_ID.getName()), Long.toString(requestId),
        "The broker matches the reply to its ping by request id");
    verify(queryScheduler, never()).submitResponse(any());
    channel.finishAndReleaseAll();
  }

  /// A server that predates pings ignores the unknown field and sees an ordinary query, which it answers once the
  /// queries queued ahead of it have run. This is the step where such a server would otherwise drop the request
  /// without replying: if it cannot parse the request into a query, it sends nothing back. (That queueing is why pings
  /// are off by default: a busy old server can miss the ping timeout.)
  @Test
  public void testServerThatIgnoresThePingFlagSeesAnAnswerableQuery()
      throws Exception {
    InstanceRequest asSeenByOldServer = QueryRouter.newPingRequest(-42L, "broker01");
    asSeenByOldServer.unsetPing();

    ServerQueryRequest queryRequest =
        new ServerQueryRequest(asSeenByOldServer, ServerMetrics.get(), System.currentTimeMillis());
    assertEquals(queryRequest.getRequestId(), -42L);

    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submitResponse(any())).thenReturn(SettableFuture.create());
    EmbeddedChannel channel = new EmbeddedChannel(
        new InstanceRequestHandler("server01", new PinotConfiguration(), queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant()));
    channel.writeInbound(Unpooled.wrappedBuffer(serialize(asSeenByOldServer)));
    verify(queryScheduler).submitResponse(any());
    channel.finishAndReleaseAll();
  }

  private static byte[] serialize(InstanceRequest instanceRequest)
      throws Exception {
    return new TSerializer(new TCompactProtocol.Factory()).serialize(instanceRequest);
  }

  @Test
  public void testCancelQuery()
      throws InterruptedException {
    PinotConfiguration config = new PinotConfiguration();
    CountDownLatch queryFinishLatch = new CountDownLatch(1);
    QueryScheduler queryScheduler = createQueryScheduler(config, queryFinishLatch);
    InstanceRequestHandler handler =
        new InstanceRequestHandler("server01", config, queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant());

    Set<String> queryIds = new HashSet<>();
    queryIds.add("foo");
    queryIds.add("bar");
    queryIds.add("baz");
    for (String queryId : queryIds) {
      ServerQueryRequest query = mock(ServerQueryRequest.class);
      when(query.getQueryId()).thenReturn(queryId);
      when(query.getTableNameWithType()).thenReturn("testTable_OFFLINE");
      QueryExecutionContext executionContext = mock(QueryExecutionContext.class);
      when(executionContext.getCid()).thenReturn(queryId);
      when(query.toExecutionContext(any())).thenReturn(executionContext);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelFuture chFu = mock(ChannelFuture.class);
      when(ctx.writeAndFlush(any())).thenReturn(chFu);
      handler.submitQuery(query, ctx, System.currentTimeMillis());
    }
    assertEquals(handler.getRunningQueryIds(), queryIds);
    for (String id : queryIds) {
      handler.cancelQuery(id);
    }
    Thread.sleep(100L);
    assertEquals(handler.getRunningQueryIds(), queryIds);
    queryFinishLatch.countDown();
    // Wait for the queries to finish and execute the callback
    TestUtils.waitForCondition((aVoid) -> handler.getRunningQueryIds().isEmpty(), 10_000L,
        "Timed out waiting for queries to finish");
    assertTrue(handler.getRunningQueryIds().isEmpty());
    assertFalse(handler.cancelQuery("unknown"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteFailureClosesChannel()
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    CountDownLatch queryFinishLatch = new CountDownLatch(1);
    QueryScheduler queryScheduler = createQueryScheduler(config, queryFinishLatch);
    InstanceRequestHandler handler =
        new InstanceRequestHandler("server01", config, queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant());

    ServerQueryRequest query = mock(ServerQueryRequest.class);
    when(query.getQueryId()).thenReturn("test-query");
    when(query.getTableNameWithType()).thenReturn("testTable_OFFLINE");
    when(query.getRequestId()).thenReturn(1L);
    QueryExecutionContext executionContext = mock(QueryExecutionContext.class);
    when(executionContext.getCid()).thenReturn("test-query");
    when(query.toExecutionContext(any())).thenReturn(executionContext);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ChannelFuture writeFuture = mock(ChannelFuture.class);
    when(ctx.writeAndFlush(any())).thenReturn(writeFuture);

    ArgumentCaptor<GenericFutureListener> listenerCaptor = ArgumentCaptor.forClass(GenericFutureListener.class);
    when(writeFuture.addListener(listenerCaptor.capture())).thenReturn(writeFuture);

    handler.submitQuery(query, ctx, System.currentTimeMillis());
    queryFinishLatch.countDown();

    TestUtils.waitForCondition((aVoid) -> !listenerCaptor.getAllValues().isEmpty(), 10_000L,
        "Timed out waiting for write listener to be registered");

    Future<Void> failedFuture = mock(Future.class);
    when(failedFuture.isSuccess()).thenReturn(false);
    when(failedFuture.cause()).thenReturn(new OutOfMemoryError("Direct buffer memory"));

    listenerCaptor.getValue().operationComplete(failedFuture);

    verify(ctx).close();
  }

  @Test
  public void testRepliesWithErrorWhenQueryCannotBeCompiled()
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    QueryScheduler queryScheduler = createQueryScheduler(config, new CountDownLatch(0));
    InstanceRequestHandler handler =
        new InstanceRequestHandler("server01", config, queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant());

    // EXPR_MIN cannot be executed without the broker-side ExprMinMaxRewriter, so the server fails to build the
    // aggregation function while it compiles the request.
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(CalciteSqlParser.compileToPinotQuery("SELECT EXPR_MIN(a, b) FROM testTable_OFFLINE"));
    InstanceRequest instanceRequest = new InstanceRequest(123L, brokerRequest);
    byte[] requestBytes = new TSerializer(new TCompactProtocol.Factory()).serialize(instanceRequest);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
    handler.channelRead0(ctx, Unpooled.wrappedBuffer(requestBytes));

    ArgumentCaptor<ByteBuf> responseCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    ByteBuf response = responseCaptor.getValue();
    byte[] responseBytes = new byte[response.readableBytes()];
    response.readBytes(responseBytes);
    DataTable dataTable = DataTableFactory.getDataTable(responseBytes);
    assertEquals(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()), "123");
    Map<Integer, String> exceptions = dataTable.getExceptions();
    assertEquals(exceptions.size(), 1);
    String message = exceptions.get(QueryErrorCode.QUERY_EXECUTION.getId());
    // Thrown by AggregationFunctionFactory for an EXPR_MIN that the broker did not rewrite
    assertTrue(message.contains("Aggregation function: EXPRMIN is only supported in selection without alias"), message);
  }

  /// A request whose broker deadline is further in the past than the clock-skew allowance is answered with a
  /// scheduling-timeout error right away and never reaches the scheduler. This is the backlog of a stalled
  /// connection being delivered after it recovers.
  @Test
  public void testRequestPastDeadlineIsAnsweredWithoutScheduling()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler handler = createHandler(new PinotConfiguration(), submitted);

    DataTable response = handle(handler, 123L, () -> Long.toString(System.currentTimeMillis() - 5_000L));

    assertTrue(submitted.isEmpty(), "an expired request must not be scheduled");
    assertEquals(response.getMetadata().get(MetadataKey.REQUEST_ID.getName()), "123");
    Map<Integer, String> exceptions = response.getExceptions();
    assertEquals(exceptions.size(), 1);
    String message = exceptions.get(QueryErrorCode.QUERY_SCHEDULING_TIMEOUT.getId());
    assertTrue(message != null && message.contains("after the broker deadline"), String.valueOf(exceptions));
  }

  /// A deadline that is in the past by less than the allowance is treated as clock skew, not as a stale request.
  @Test
  public void testRequestWithinClockSkewAllowanceIsExecuted()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler handler = createHandler(new PinotConfiguration(), submitted);
    // Warm up the compile and request path so that the second request's offset is not eaten by class loading
    handle(handler, 1L, null);

    DataTable response = handle(handler, 2L,
        () -> Long.toString(System.currentTimeMillis() - Server.DEFAULT_REQUEST_DEADLINE_CLOCK_SKEW_MS / 2));

    assertEquals(submitted.size(), 2);
    assertTrue(response.getExceptions().isEmpty(), String.valueOf(response.getExceptions()));
  }

  /// Old brokers send no deadline, and a non-positive one means the same: the request is executed as before.
  @Test
  public void testRequestWithoutDeadlineIsExecuted()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler handler = createHandler(new PinotConfiguration(), submitted);

    assertTrue(handle(handler, 1L, null).getExceptions().isEmpty());
    assertTrue(handle(handler, 2L, () -> "0").getExceptions().isEmpty());
    assertTrue(handle(handler, 3L, () -> "-1").getExceptions().isEmpty());
    assertEquals(submitted.size(), 3);
  }

  /// A huge deadline, which only an old broker forwarding a user's value can send, is far in the future rather than
  /// overflowing into a rejection: the check subtracts the deadline from the arrival time instead of adding the
  /// allowance to the deadline.
  @Test
  public void testHugeDeadlineIsExecuted()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler handler = createHandler(new PinotConfiguration(), submitted);

    assertTrue(handle(handler, 1L, () -> Long.toString(Long.MAX_VALUE)).getExceptions().isEmpty());
    assertTrue(handle(handler, 2L, () -> Long.toString(Long.MAX_VALUE - 1)).getExceptions().isEmpty());
    assertEquals(submitted.size(), 2);
  }

  /// A value that is not a number (only an old broker forwards user input unchanged) is answered with the parse error
  /// under the request's own id, so the broker does not wait for the query to time out, and is not scheduled.
  @Test
  public void testMalformedDeadlineIsAnsweredWithParseError()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler handler = createHandler(new PinotConfiguration(), submitted);

    DataTable response = handle(handler, 7L, () -> "soon");

    assertTrue(submitted.isEmpty());
    Map<Integer, String> exceptions = response.getExceptions();
    assertEquals(exceptions.keySet(), Set.of(QueryErrorCode.QUERY_EXECUTION.getId()));
    assertTrue(exceptions.get(QueryErrorCode.QUERY_EXECUTION.getId()).contains(QueryOptionKey.REQUEST_DEADLINE_MS),
        String.valueOf(exceptions));
  }

  /// A negative allowance switches the check off; a zero allowance drops any request past its deadline.
  @Test
  public void testClockSkewAllowanceConfig()
      throws Exception {
    List<ServerQueryRequest> submitted = new ArrayList<>();
    InstanceRequestHandler disabled = createHandler(
        new PinotConfiguration(Map.of(Server.CONFIG_OF_REQUEST_DEADLINE_CLOCK_SKEW_MS, "-1")), submitted);
    assertTrue(handle(disabled, 1L, () -> Long.toString(System.currentTimeMillis() - 60_000L)).getExceptions()
        .isEmpty());
    assertEquals(submitted.size(), 1);

    InstanceRequestHandler strict = createHandler(
        new PinotConfiguration(Map.of(Server.CONFIG_OF_REQUEST_DEADLINE_CLOCK_SKEW_MS, "0")), submitted);
    DataTable response = handle(strict, 2L, () -> Long.toString(System.currentTimeMillis() - 50L));
    assertEquals(submitted.size(), 1, "a request past its deadline must not be scheduled with a zero allowance");
    assertEquals(response.getExceptions().keySet(), Set.of(QueryErrorCode.QUERY_SCHEDULING_TIMEOUT.getId()));
  }

  /// A handler whose scheduler records what it is given and answers each request with an empty data table.
  private InstanceRequestHandler createHandler(PinotConfiguration config, List<ServerQueryRequest> submitted) {
    ResourceManager resourceManager = new UnboundedResourceManager(config);
    QueryScheduler queryScheduler =
        new QueryScheduler(config, "serverId", mock(QueryExecutor.class), ThreadAccountantUtils.getNoOpAccountant(),
            new LongAccumulator(Long::max, 0), resourceManager) {
          @Override
          public ListenableFuture<DataBuffer> submitResponse(ServerQueryRequest queryRequest) {
            submitted.add(queryRequest);
            DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
            dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(queryRequest.getRequestId()));
            try {
              return Futures.immediateFuture(dataTable.toDataBuffer());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public String name() {
            return "recording";
          }
        };
    return new InstanceRequestHandler("server01", config, queryScheduler, mock(AccessControl.class),
        ThreadAccountantUtils.getNoOpAccountant());
  }

  /// Feeds the handler a serialized request for `testTable_OFFLINE` with a 10s timeout and the `requestDeadlineMs`
  /// option the supplier gives (none when the supplier is null), and returns the data table it wrote back. The
  /// supplier is read after the SQL has been compiled, right before the request is handed over, so that a slow first
  /// compile cannot eat into an offset relative to the current time.
  private static DataTable handle(InstanceRequestHandler handler, long requestId,
      @Nullable Supplier<String> requestDeadlineMs)
      throws Exception {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT * FROM testTable_OFFLINE");
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.TIMEOUT_MS, "10000");
    if (requestDeadlineMs != null) {
      queryOptions.put(QueryOptionKey.REQUEST_DEADLINE_MS, requestDeadlineMs.get());
    }
    pinotQuery.setQueryOptions(queryOptions);
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    byte[] requestBytes =
        new TSerializer(new TCompactProtocol.Factory()).serialize(new InstanceRequest(requestId, brokerRequest));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
    handler.channelRead0(ctx, Unpooled.wrappedBuffer(requestBytes));

    ArgumentCaptor<ByteBuf> responseCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    ByteBuf response = responseCaptor.getValue();
    byte[] responseBytes = new byte[response.readableBytes()];
    response.readBytes(responseBytes);
    DataTable dataTable = DataTableFactory.getDataTable(responseBytes);
    assertEquals(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()), Long.toString(requestId));
    return dataTable;
  }

  private QueryScheduler createQueryScheduler(PinotConfiguration config, CountDownLatch queryFinishLatch) {
    ResourceManager resourceManager = new UnboundedResourceManager(config);
    return new QueryScheduler(config, "serverId", mock(QueryExecutor.class), ThreadAccountantUtils.getNoOpAccountant(),
        new LongAccumulator(Long::max, 0), resourceManager) {
      @Override
      public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
        // Create a FutureTask does nothing but waits to be cancelled and trigger callbacks.
        ListenableFutureTask<byte[]> task = ListenableFutureTask.create(() -> {
          queryFinishLatch.await();
          return null;
        });
        resourceManager.getQueryRunners().submit(task);
        return task;
      }

      @Override
      public String name() {
        return "noop";
      }
    };
  }
}
