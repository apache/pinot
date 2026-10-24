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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.NativeQueryIdentity;
import org.apache.pinot.common.request.NativeQueryObject;
import org.apache.pinot.common.request.NativeQueryProcess;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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

  @Test
  public void testRepliesWithValidationErrorForMalformedNativeContext()
      throws Exception {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    InstanceRequestHandler handler =
        new InstanceRequestHandler("server01", new PinotConfiguration(), queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant());
    String id = UUID.randomUUID().toString();
    NativeQueryIdentity identity =
        new NativeQueryIdentity(1, "invalid-history", new NativeQueryProcess("broker", id, 1),
            new NativeQueryProcess("server", id, 1), id, id, "testTable_OFFLINE", new NativeQueryObject(id, 1),
            "testTable",
            new NativeQueryObject(id, 1), List.of(), List.of());
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(CalciteSqlParser.compileToPinotQuery("SELECT COUNT(*) FROM testTable_OFFLINE"));
    InstanceRequest instanceRequest = new InstanceRequest(124L, brokerRequest);
    instanceRequest.setNativeQueryContext(identity);
    byte[] requestBytes = new TSerializer(new TCompactProtocol.Factory()).serialize(instanceRequest);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
    ByteBuf input = Unpooled.wrappedBuffer(requestBytes);
    try {
      handler.channelRead0(ctx, input);
    } finally {
      input.release();
    }
    ArgumentCaptor<ByteBuf> responseCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    ByteBuf response = responseCaptor.getValue();
    try {
      byte[] responseBytes = new byte[response.readableBytes()];
      response.readBytes(responseBytes);
      DataTable dataTable = DataTableFactory.getDataTable(responseBytes);
      assertEquals(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()), "124");
      assertEquals(dataTable.getExceptions().size(), 1);
      assertTrue(dataTable.getExceptions().containsKey(QueryErrorCode.QUERY_VALIDATION.getId()),
          dataTable.getExceptions().toString());
      verifyNoInteractions(queryScheduler);
    } finally {
      response.release();
    }
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
