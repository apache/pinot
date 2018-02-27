/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.netty;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.AsyncResponseFuture;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.NoneType;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Netty standalone connection. This will be managed as a resource in a pool to reuse
 * connection. This is not thread-safe so multiple requests cannot be sent simultaneously.
 * This class provides an async API to send requests and wait for response.
 */
public abstract class NettyClientConnection {
  protected static Logger LOGGER = LoggerFactory.getLogger(NettyTCPClientConnection.class);

  /**
   * Client Connection State
   */
  public enum State {
    INIT,
    CONNECTED,
    REQUEST_WRITTEN,
    REQUEST_SENT,
    ERROR,
    GOT_RESPONSE;

    public boolean isValidTransition(State nextState) {
      switch (nextState) {
        case INIT:
          return false; // Init state happens only as the first transition
        case CONNECTED:
          return this == State.INIT; // We do not reconnect with same NettyClientConnection object. We create new one
        case REQUEST_WRITTEN:
          return (this == State.CONNECTED) || (this == State.GOT_RESPONSE);
        case REQUEST_SENT:
          return this == State.REQUEST_WRITTEN;
        case ERROR:
          return true;
        case GOT_RESPONSE:
          return this == State.REQUEST_SENT;
      }
      return false;
    }
  };

  protected final ServerInstance _server;

  protected final EventLoopGroup _eventGroup;
  protected Bootstrap _bootstrap;
  protected volatile Channel _channel;
  // State of the request/connection
  protected volatile State _connState;
  protected final long _connId;

  // Timer for tracking read-timeouts
  protected final Timer _timer;

  // Callback to notify if a response has been successfully received or error
  protected volatile Callback<NoneType> _requestCallback;

  public NettyClientConnection(ServerInstance server, EventLoopGroup eventGroup, Timer timer, long connId) {
    _connState = State.INIT;
    _server = server;
    _connId = connId;
    _timer = timer;
    _eventGroup = eventGroup;
  }

  public long getConnId() {
    return _connId;
  }


  /**
   * Connect to the server. Returns false if unable to connect to the server.
   */
  public abstract boolean connect();

  /**
   * Close the client connection
   */
  public abstract void close() throws InterruptedException;

  /**
   * API to send a request asynchronously.
   * @param serializedRequest serialized payload to send the request
   * @param requestId Request Id
   * @param timeoutMs Timeout in milli-seconds. If timeout &lt; 0, then no timeout
   * @return Future to return the response returned from the server.
   */
  public abstract ResponseFuture sendRequest(ByteBuf serializedRequest, long requestId, long timeoutMs);

  public void setRequestCallback(Callback<NoneType> callback) {
    _requestCallback = callback;
  }

  /**
   * Future Handle provided to the request sender to asynchronously wait for response.
   * We use guava API for implementing Futures.
   */
  public static class ResponseFuture extends AsyncResponseFuture<byte[]> {

    public ResponseFuture(ServerInstance key, String ctxt) {
      super(key, ctxt);
    }

    /**
     * Error Future which is used to represent a future which failed with error.
     * @param key Key for the future
     * @param error Throwable for the response
     */
    public ResponseFuture(ServerInstance key, Throwable error, String ctxt) {
      super(key, error, ctxt);
    }
  }

  /**
   * Validates if the underlying channel is active
   * @return
   */
  public boolean validate() {
    if (null == _channel) {
      return false;
    }
    return _channel.isActive();
  }

  public ServerInstance getServer() {
    return _server;
  }
}
