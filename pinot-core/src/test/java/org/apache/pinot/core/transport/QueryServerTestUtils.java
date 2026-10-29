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
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Function;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;

import static org.mockito.Mockito.mock;


/// Helpers for standing up a real Netty [QueryServer] in a test.
///
/// Lives in `org.apache.pinot.core.transport` because [QueryServer#getChannel] and the `host`/`port`
/// [ServerInstance] constructor are package-private; pinot-broker consumes this through the pinot-core test jar.
public class QueryServerTestUtils {
  private QueryServerTestUtils() {
  }

  /// Builds an unstarted query server whose scheduler answers each query with the future `responder` returns for it.
  ///
  /// `responder` runs on the server's network threads, which also answer pings. To model a slow or stuck query
  /// engine, return a future that completes later or never: a `responder` that blocks holds back pings as well.
  public static QueryServer newQueryServer(Function<ServerQueryRequest, ListenableFuture<DataBuffer>> responder) {
    return newQueryServer(0, responder);
  }

  /// Same as [#newQueryServer(Function)], on a given port -- for a test that brings a server back where it was.
  public static QueryServer newQueryServer(int port,
      Function<ServerQueryRequest, ListenableFuture<DataBuffer>> responder) {
    PinotConfiguration config = new PinotConfiguration();
    // A subclass, not a mock: the scheduler sends both submit methods to the one overridden here, so the server gets
    // this answer whichever one it calls. A mock would answer null from the one not stubbed.
    QueryScheduler queryScheduler = new QueryScheduler(config, "testServer", mock(QueryExecutor.class),
        ThreadAccountantUtils.getNoOpAccountant(), new LongAccumulator(Long::max, 0), mock(ResourceManager.class)) {
      @Override
      public ListenableFuture<DataBuffer> submitResponse(ServerQueryRequest queryRequest) {
        return responder.apply(queryRequest);
      }

      @Override
      public String name() {
        return "test";
      }
    };
    InstanceRequestHandler handler = new InstanceRequestHandler("testServer", config, queryScheduler,
        mock(AccessControl.class), ThreadAccountantUtils.getNoOpAccountant());
    return new QueryServer(port, null, handler);
  }

  /// Starts the server and returns the port it bound to.
  ///
  /// Binds on port 0 and reads the port back, rather than picking a free port up front, which would leave a window
  /// for another process on the machine to take it. [QueryServer#start] blocks on `bind().sync()`, so the server is
  /// accepting connections when this returns.
  public static int startAndGetPort(QueryServer server) {
    server.start();
    return ((InetSocketAddress) server.getChannel().localAddress()).getPort();
  }

  public static ServerInstance serverInstance(String hostname, int port) {
    return new ServerInstance(hostname, port);
  }
}
