package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.transport.common.AsyncResponseFuture;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.common.ServerInstance;

/**
 * A Netty standalone connection. This will be managed as a resource in a pool to reuse
 * connection. This is not thread-safe so multiple requests cannot be sent simultaneously.
 * This class provides an async API to send requests and wait for response.
 */
public abstract class NettyClientConnection
{
  protected static Logger LOG = LoggerFactory.getLogger(NettyTCPClientConnection.class);

  /**
   * Client Connection State
   */
  public enum State
  {
    INIT,
    CONNECTED,
    REQUEST_WRITTEN,
    REQUEST_SENT,
    ERROR,
    GOT_RESPONSE;

    public boolean isValidTransition(State nextState)
    {
      switch (nextState)
      {
        case INIT: return false; // Init state happens only as the first transition
        case CONNECTED: return this == State.INIT;  // We do not reconnect with same NettyClientConnection object. We create new one
        case REQUEST_WRITTEN: return (this == State.CONNECTED) || (this == State.GOT_RESPONSE);
        case REQUEST_SENT: return this == State.REQUEST_WRITTEN;
        case ERROR: return true;
        case GOT_RESPONSE: return this == State.REQUEST_SENT;
      }
      return false;
    }
  };

  protected final ServerInstance _server;

  protected final EventLoopGroup _eventGroup;
  protected Bootstrap _bootstrap;
  protected Channel _channel;
  // State of the request/connection
  protected State _connState;

  // Callback to notify if a response has been successfully received or error
  protected Callback<NoneType> _requestCallback;

  public NettyClientConnection(ServerInstance server, EventLoopGroup eventGroup)
  {
    _connState = State.INIT;
    _server = server;
    _eventGroup = eventGroup;
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
   * @return Future to return the response returned from the server.
   */
  public abstract ResponseFuture sendRequest(ByteBuf serializedRequest);


  public void setRequestCallback(Callback<NoneType> callback) {
    _requestCallback = callback;
  }


  /**
   * Future Handle provided to the request sender to asynchronously wait for response.
   * We use guava API for implementing Futures.
   */
  public static class ResponseFuture extends AsyncResponseFuture<ServerInstance, ByteBuf>
  {

    public ResponseFuture(ServerInstance key) {
      super(key);
    }

  }

  /**
   * Validates if the underlying channel is active
   * @return
   */
  public boolean validate()
  {
    if (null == _channel) {
      return false;
    }
    return _channel.isActive();
  }

  public ServerInstance getServer() {
    return _server;
  }
}

