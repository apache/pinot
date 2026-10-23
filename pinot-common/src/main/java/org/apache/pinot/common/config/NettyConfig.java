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
package org.apache.pinot.common.config;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.env.PinotConfiguration;


/// Container object for netty configuration of pinot clients and servers (netty, grizzly, etc.)
///
/// Every setting is read as `<namespace>.<suffix>`, where the namespace is `pinot.broker.netty` on the broker and
/// `pinot.server.netty` on the server. The channel liveness settings below only apply to the client side, i.e. the
/// broker-to-server channels opened by `ServerChannels`; the server-side `QueryServer` ignores them.
public class NettyConfig {
  private static final String NATIVE_TRANSPORTS_ENABLED = "native.transports.enabled";

  /// Upper bound on how long a request write may stay pending before the channel is closed, in milliseconds. A write
  /// is pending while the kernel has not accepted the bytes, which on a half-open or stalled socket is forever (the
  /// peer stops acknowledging, the send buffer fills, and `isActive()` keeps reporting `true` until the kernel gives
  /// up on retransmits, typically after many minutes). Closing the channel fails the in-flight queries on it fast
  /// and lets the failure detector quarantine the server. Non-positive disables the check. Broker key:
  /// `pinot.broker.netty.write.timeout.ms`.
  public static final String WRITE_TIMEOUT_MS = "write.timeout.ms";
  public static final long DEFAULT_WRITE_TIMEOUT_MS = 10_000L;

  /// Linux `TCP_USER_TIMEOUT`, in milliseconds: how long transmitted data may remain unacknowledged before the
  /// kernel closes the connection, replacing the default retransmission timeout of several minutes. Covers the
  /// case the write timeout cannot see: small writes that the kernel accepted into a send buffer that the peer never
  /// drains. Only applied when the broker uses the Epoll transport (`native.transports.enabled` on Linux); NIO and
  /// KQueue have no such socket option. Non-positive leaves the kernel default. Broker key:
  /// `pinot.broker.netty.tcp.user.timeout.ms`.
  public static final String TCP_USER_TIMEOUT_MS = "tcp.user.timeout.ms";
  public static final int DEFAULT_TCP_USER_TIMEOUT_MS = 10_000;

  /// Netty per-channel outbound queue high watermark, in bytes. Once the bytes queued for a channel exceed it,
  /// `Channel.isWritable()` turns `false` and `ServerChannels` rejects further requests to that server instead of
  /// queueing them, so a closed send window costs at most this much direct memory per channel rather than growing
  /// without bound. Requests are small, and a healthy peer drains its socket at line rate, so the queue only reaches
  /// this depth when the peer or the path has stalled. Broker key:
  /// `pinot.broker.netty.write.buffer.high.water.mark.bytes`.
  public static final String WRITE_BUFFER_HIGH_WATER_MARK_BYTES = "write.buffer.high.water.mark.bytes";
  public static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK_BYTES = 16 * 1024 * 1024;

  /// Netty per-channel outbound queue low watermark, in bytes: once unwritable, the queue must drain below this
  /// before the channel accepts requests again. Must not exceed the high watermark. Broker key:
  /// `pinot.broker.netty.write.buffer.low.water.mark.bytes`.
  public static final String WRITE_BUFFER_LOW_WATER_MARK_BYTES = "write.buffer.low.water.mark.bytes";
  public static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK_BYTES = 8 * 1024 * 1024;

  private boolean _nativeTransportsEnabled = false;
  private long _writeTimeoutMs = DEFAULT_WRITE_TIMEOUT_MS;
  private int _tcpUserTimeoutMs = DEFAULT_TCP_USER_TIMEOUT_MS;
  private int _writeBufferHighWaterMarkBytes = DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK_BYTES;
  private int _writeBufferLowWaterMarkBytes = DEFAULT_WRITE_BUFFER_LOW_WATER_MARK_BYTES;

  private static String key(String namespace, String suffix) {
    return namespace + "." + suffix;
  }

  public static NettyConfig extractNettyConfig(PinotConfiguration pinotConfig, String namespace) {
    return NettyConfig.extractNettyConfig(pinotConfig, namespace, new NettyConfig());
  }

  public static NettyConfig extractNettyConfig(PinotConfiguration pinotConfig, String namespace,
      NettyConfig defaultConfig) {

    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(pinotConfig.getProperty(key(namespace, NATIVE_TRANSPORTS_ENABLED),
        defaultConfig.isNativeTransportsEnabled()));
    nettyConfig.setWriteTimeoutMs(
        pinotConfig.getProperty(key(namespace, WRITE_TIMEOUT_MS), defaultConfig.getWriteTimeoutMs()));
    nettyConfig.setTcpUserTimeoutMs(
        pinotConfig.getProperty(key(namespace, TCP_USER_TIMEOUT_MS), defaultConfig.getTcpUserTimeoutMs()));
    int highWaterMark = pinotConfig.getProperty(key(namespace, WRITE_BUFFER_HIGH_WATER_MARK_BYTES),
        defaultConfig.getWriteBufferHighWaterMarkBytes());
    int lowWaterMark = pinotConfig.getProperty(key(namespace, WRITE_BUFFER_LOW_WATER_MARK_BYTES),
        defaultConfig.getWriteBufferLowWaterMarkBytes());
    nettyConfig.setWriteBufferWaterMarks(lowWaterMark, highWaterMark);

    return nettyConfig;
  }

  public boolean isNativeTransportsEnabled() {
    return _nativeTransportsEnabled;
  }

  public void setNativeTransportsEnabled(boolean nativeTransportsEnabled) {
    _nativeTransportsEnabled = nativeTransportsEnabled;
  }

  public long getWriteTimeoutMs() {
    return _writeTimeoutMs;
  }

  public void setWriteTimeoutMs(long writeTimeoutMs) {
    _writeTimeoutMs = writeTimeoutMs;
  }

  public int getTcpUserTimeoutMs() {
    return _tcpUserTimeoutMs;
  }

  public void setTcpUserTimeoutMs(int tcpUserTimeoutMs) {
    _tcpUserTimeoutMs = tcpUserTimeoutMs;
  }

  public int getWriteBufferHighWaterMarkBytes() {
    return _writeBufferHighWaterMarkBytes;
  }

  public int getWriteBufferLowWaterMarkBytes() {
    return _writeBufferLowWaterMarkBytes;
  }

  /// Sets both watermarks together because they constrain each other: the high watermark must be positive and the
  /// low watermark must be between zero and the high watermark, which is also what Netty's `WriteBufferWaterMark`
  /// enforces.
  public void setWriteBufferWaterMarks(int lowWaterMarkBytes, int highWaterMarkBytes) {
    Preconditions.checkArgument(highWaterMarkBytes > 0, "Write buffer high watermark must be positive, got: %s",
        highWaterMarkBytes);
    Preconditions.checkArgument(lowWaterMarkBytes >= 0 && lowWaterMarkBytes <= highWaterMarkBytes,
        "Write buffer low watermark must be between 0 and the high watermark (%s), got: %s", highWaterMarkBytes,
        lowWaterMarkBytes);
    _writeBufferLowWaterMarkBytes = lowWaterMarkBytes;
    _writeBufferHighWaterMarkBytes = highWaterMarkBytes;
  }
}
