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


/**
 * Process-wide shared Netty resources for Pinot client {@code AsyncHttpClient} instances.
 *
 * <p>Every {@code AsyncHttpClient} built inside the Pinot client (one per {@link BrokerCache},
 * per query transport, and per controller transport) owns, by default, its own
 * {@link io.netty.channel.EventLoopGroup} and {@link io.netty.util.HashedWheelTimer}. When many
 * client connections are kept alive simultaneously (for example behind a JDBC connection pool
 * such as HikariCP), this multiplies the number of Netty I/O and timer threads by the number of
 * pooled connections. The combination of AHC's default pooled-connection idle timeout (60s) and
 * the broker cache refresh interval (5 minutes) additionally causes new TCP connections to be
 * opened on every refresh, which lazily spawns fresh {@code NioEventLoop} threads up to each
 * group's {@code 2 * availableProcessors()} cap.
 *
 * <p>AsyncHttpClient natively supports injecting an externally managed {@code EventLoopGroup}
 * and {@code Timer} via
 * {@link org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder#setEventLoopGroup(EventLoopGroup)}
 * and {@link org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder#setNettyTimer(Timer)}; when
 * either is supplied externally, AHC will not shut it down on {@code close()} (see
 * {@code ChannelManager#allowReleaseEventLoopGroup} and
 * {@code DefaultAsyncHttpClient#allowStopNettyTimer}). This class exposes such shared instances
 * so that Pinot client AHCs can reuse a single I/O thread pool and a single timer across the
 * JVM regardless of how many {@code AsyncHttpClient} instances are created.
 *
 * <p>The underlying threads are daemon threads; no explicit shutdown is required and they will
 * not block JVM exit.
 */
public final class PinotClientNettyResources {

  private PinotClientNettyResources() {
  }

  private static final class Holder {
    private static final EventLoopGroup EVENT_LOOP_GROUP =
        new NioEventLoopGroup(0, new DefaultThreadFactory("pinot-client-nio", true));
    private static final Timer TIMER =
        new HashedWheelTimer(new DefaultThreadFactory("pinot-client-timer", true));
  }

  /**
   * Returns the JVM-wide shared {@link EventLoopGroup} for Pinot client {@code AsyncHttpClient}
   * instances. The group uses daemon threads and the Netty default size of
   * {@code 2 * availableProcessors()}.
   */
  public static EventLoopGroup eventLoopGroup() {
    return Holder.EVENT_LOOP_GROUP;
  }

  /**
   * Returns the JVM-wide shared {@link Timer} for Pinot client {@code AsyncHttpClient} instances.
   * The timer uses a daemon thread.
   */
  public static Timer timer() {
    return Holder.TIMER;
  }
}
