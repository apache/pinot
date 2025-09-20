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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InstanceRequestHandlerTest {

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
