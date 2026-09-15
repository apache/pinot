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
package org.apache.pinot.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;


/// Process-wide shared Netty resources for Pinot client `AsyncHttpClient` instances.
///
/// Every `AsyncHttpClient` built inside the Pinot client (one per [BrokerCache],
/// per query transport, and per controller transport) owns, by default, its own
/// [io.netty.channel.EventLoopGroup] and [io.netty.util.HashedWheelTimer]. When many
/// client connections are kept alive simultaneously (for example behind a JDBC connection pool
/// such as HikariCP), this multiplies the number of Netty I/O and timer threads by the number of
/// pooled connections. The combination of AHC's default pooled-connection idle timeout (60s) and
/// the broker cache refresh interval (5 minutes) additionally causes new TCP connections to be
/// opened on every refresh, which lazily spawns fresh `NioEventLoop` threads up to each
/// group's `2 * availableProcessors()` cap.
///
/// AsyncHttpClient natively supports injecting an externally managed `EventLoopGroup`
/// and `Timer` via
/// [org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder#setEventLoopGroup(EventLoopGroup)]
/// and [org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder#setNettyTimer(Timer)]; when
/// either is supplied externally, AHC will not shut it down on `close()` (see
/// `ChannelManager#allowReleaseEventLoopGroup` and
/// `DefaultAsyncHttpClient#allowStopNettyTimer`). This class exposes such shared instances
/// so that Pinot client AHCs can reuse a single I/O thread pool and a single timer across the
/// JVM regardless of how many `AsyncHttpClient` instances are created.
///
/// The underlying threads are daemon threads; no explicit shutdown is required and they will
/// not block JVM exit.
public final class PinotClientNettyResources {

  private PinotClientNettyResources() {
  }

  private static final class Holder {
    private static final EventLoopGroup EVENT_LOOP_GROUP =
        new NioEventLoopGroup(0, new DefaultThreadFactory("pinot-client-nio", true));
    private static final Timer TIMER =
        new HashedWheelTimer(new DefaultThreadFactory("pinot-client-timer", true));
  }

  /// Returns the JVM-wide shared [EventLoopGroup] for Pinot client `AsyncHttpClient`
  /// instances. The group uses daemon threads and the Netty default size of
  /// `2 * availableProcessors()`.
  public static EventLoopGroup eventLoopGroup() {
    return Holder.EVENT_LOOP_GROUP;
  }

  /// Returns the JVM-wide shared [Timer] for Pinot client `AsyncHttpClient` instances.
  /// The timer uses a daemon thread.
  public static Timer timer() {
    return Holder.TIMER;
  }
}
