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
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class InstanceRequestHandlerTest {
  @Test
  public void testCancelQuery() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("pinot.server.enable.query.cancellation", "true");
    QueryScheduler qs = createQueryScheduler(config);
    InstanceRequestHandler handler =
        new InstanceRequestHandler("server01", config, qs, mock(ServerMetrics.class), mock(AccessControl.class));

    Set<String> queryIds = new HashSet<>();
    queryIds.add("foo");
    queryIds.add("bar");
    queryIds.add("baz");
    for (String id : queryIds) {
      ServerQueryRequest query = mock(ServerQueryRequest.class);
      when(query.getQueryId()).thenReturn(id);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelFuture chFu = mock(ChannelFuture.class);
      when(ctx.writeAndFlush(any())).thenReturn(chFu);
      handler.submitQuery(query, ctx, "myTable01", System.currentTimeMillis(), mock(InstanceRequest.class));
    }
    Assert.assertEquals(handler.getRunningQueryIds(), queryIds);
    for (String id : queryIds) {
      handler.cancelQuery(id);
    }
    Assert.assertTrue(handler.getRunningQueryIds().isEmpty());
    Assert.assertFalse(handler.cancelQuery("unknown"));
  }

  private QueryScheduler createQueryScheduler(PinotConfiguration config) {
    return new QueryScheduler(config, mock(QueryExecutor.class), mock(ResourceManager.class), mock(ServerMetrics.class),
        new LongAccumulator(Long::max, 0)) {
      @Override
      public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
        // Create a FutureTask does nothing but waits to be cancelled and trigger callbacks.
        return ListenableFutureTask.create(() -> null);
      }

      @Override
      public String name() {
        return "noop";
      }
    };
  }
}
