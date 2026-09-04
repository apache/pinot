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

import java.net.InetSocketAddress;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Helpers for standing up a real Netty [QueryServer] in a test.
///
/// Lives in `org.apache.pinot.core.transport` because [QueryServer#getChannel] and the `host`/`port`
/// [ServerInstance] constructor are package-private; pinot-broker consumes this through the pinot-core test jar.
public class QueryServerTestUtils {
  private QueryServerTestUtils() {
  }

  /// Builds an unstarted query server whose scheduler answers every submission with `submitAnswer`.
  public static QueryServer newQueryServer(Answer<?> submitAnswer) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(submitAnswer);
    InstanceRequestHandler handler =
        new InstanceRequestHandler("testServer", new PinotConfiguration(), queryScheduler, mock(AccessControl.class),
            ThreadAccountantUtils.getNoOpAccountant());
    return new QueryServer(0, null, handler);
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
