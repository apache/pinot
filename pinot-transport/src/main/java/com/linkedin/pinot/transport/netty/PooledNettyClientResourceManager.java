package com.linkedin.pinot.transport.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.PooledResourceManager;


public class PooledNettyClientResourceManager implements PooledResourceManager<ServerInstance, NettyClientConnection> {

  protected static Logger LOG = LoggerFactory.getLogger(PooledNettyClientResourceManager.class);

  private KeyedPool<ServerInstance, NettyClientConnection> _pool;
  private final EventLoopGroup _eventLoop;
  private final NettyClientMetrics _metrics;
  private final Timer _timer;

  public PooledNettyClientResourceManager(EventLoopGroup eventLoop, Timer timer, NettyClientMetrics metrics) {
    _eventLoop = eventLoop;
    _metrics = metrics;
    _timer = timer;
  }

  public void setPool(KeyedPool<ServerInstance, NettyClientConnection> pool) {
    _pool = pool;
  }

  @Override
  public NettyClientConnection create(ServerInstance key) {
    NettyClientConnection conn = new PooledClientConnection(_pool, key, _eventLoop, _timer, _metrics);
    conn.connect();
    return conn;
  }

  @Override
  public boolean destroy(ServerInstance key, boolean isBad, NettyClientConnection resource) {

    LOG.info("Destroying client connection to server :" + key);
    boolean closed = false;
    try {
      resource.close();
      closed = true;
    } catch (InterruptedException e) {
      LOG.error("Got interrupted exception when closing resource", e);
    }

    return closed;
  }

  @Override
  public boolean validate(ServerInstance key, NettyClientConnection resource) {
    return resource.validate();
  }

  /**
   * Pool aware NettyTCPClientConnection
   *
   */
  public class PooledClientConnection extends NettyTCPClientConnection implements Callback<NoneType> {
    private final KeyedPool<ServerInstance, NettyClientConnection> _pool;

    public PooledClientConnection(KeyedPool<ServerInstance, NettyClientConnection> pool, ServerInstance server,
        EventLoopGroup eventGroup, Timer timer, NettyClientMetrics metric) {
      super(server, eventGroup, timer, metric);
      _pool = pool;
      init();
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
      LOG.error("Got error for the netty client connection. Destroing the connection", arg0);
      /**
       * We got error. Time to discard this connection.
       */
      _pool.destroyObject(getServer(), this);
    }
  }
}
