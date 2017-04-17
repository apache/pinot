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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.PooledResourceManager;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;


public class PooledNettyClientResourceManager implements PooledResourceManager<PooledNettyClientResourceManager.PooledClientConnection> {

  protected static Logger LOGGER = LoggerFactory.getLogger(PooledNettyClientResourceManager.class);

  private KeyedPool<PooledClientConnection> _pool;
  private final EventLoopGroup _eventLoop;
  private final NettyClientMetrics _metrics;
  private final Timer _timer;

  public PooledNettyClientResourceManager(EventLoopGroup eventLoop, Timer timer, NettyClientMetrics metrics) {
    _eventLoop = eventLoop;
    _metrics = metrics;
    _timer = timer;
  }

  public void setPool(KeyedPool<PooledClientConnection> pool) {
    _pool = pool;
  }

  @Override
  public PooledClientConnection create(ServerInstance key) {
    PooledClientConnection conn = new PooledClientConnection(_pool, key, _eventLoop, _timer, _metrics);
    conn.connect();
    // At this point, we have already waited for a connection to complete. Whether it succeeds or fails,
    // we should return the object to the pool. It is possible to return null if the connection attempt
    // fails (i.e. we get back a false return above), but then the pool starts to initiate new connections
    // to the host to maintain the minimum pool size. We don't want to DOS the server continuously
    // trying to create new connections. A connection is always validated before the pool returns it
    // from the idle list to the user.
    return conn;
  }

  // This destroy method is called ONLY when asyncpoolimpl wants to destroy the connection object.
  @Override
  public boolean destroy(ServerInstance key, boolean isBad, PooledClientConnection resource) {
    LOGGER.info("Destroying client connection {}, isBad: {}", resource, isBad);
    boolean closed = false;
    try {
      resource.close();
      resource.setDestroyed(true);
      closed = true;
    } catch (InterruptedException e) {
      LOGGER.error("Got interrupted exception when closing resource {}", resource, e);
    }

    return closed;
  }

  @Override
  public boolean validate(ServerInstance key, PooledClientConnection resource) {
    if ( null != resource)
      return resource.validate();
    return false;
  }

  /**
   * Pool aware NettyTCPClientConnection
   *
   */
  public class PooledClientConnection extends NettyTCPClientConnection implements Callback<NoneType> {
    private final KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _pool;
    private boolean _destroyed = false;

    public PooledClientConnection(KeyedPool<PooledClientConnection> pool, ServerInstance server,
        EventLoopGroup eventGroup, Timer timer, NettyClientMetrics metric) {
      super(server, eventGroup, timer, metric);
      _pool = pool;
      init();
    }

    public void setDestroyed(boolean isDestroyed) {
      _destroyed = isDestroyed;
    }

    public void init() {
      setRequestCallback(this);
    }

    @Override
    public void onSuccess(NoneType arg0) {
      /**
       * We got the response successfully. Time to checkin back to the pool.
       */
      _pool.checkinObject(getServer(), this);
    }

    @Override
    public void onError(Throwable arg0) {
      if (isSelfClose()) {
        LOGGER.info("Got notified on self-close to {} connId {}", _server, getConnId());
      }
      /**
       * We got error. Time to discard this connection.
       */
      if (!_destroyed) {
        LOGGER.info("Destroying connection (onError) {} due to error connId {}", _server, getConnId(),
            arg0.getMessage());
        _pool.destroyObject(getServer(), this);
        _destroyed = true;
      }
    }

    @Override
    protected void releaseResources() {
      // _pool is null in tests only.
      if (_pool != null && !_destroyed) {
        LOGGER.info("Destroying connection (releaseResources) {} due to error connId {}", _server, getConnId());
        _pool.destroyObject(getServer(), this);
        _destroyed = true;
      }
    }
  }
}
