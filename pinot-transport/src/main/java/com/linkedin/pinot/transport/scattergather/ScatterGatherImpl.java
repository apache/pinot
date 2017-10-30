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
package com.linkedin.pinot.transport.scattergather;

import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;
import com.linkedin.pinot.transport.common.ServerResponseFuture;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Scatter-Gather implementation
 *
 */
public class ScatterGatherImpl implements ScatterGather {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScatterGatherImpl.class);

  private final KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _connPool;
  private final ExecutorService _executorService;

  public ScatterGatherImpl(@Nonnull KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connPool,
      @Nonnull ExecutorService executorService) {
    _connPool = connPool;
    _executorService = executorService;
  }

  @Nonnull
  @Override
  public CompositeFuture<byte[]> scatterGather(@Nonnull ScatterGatherRequest scatterGatherRequest,
      @Nonnull ScatterGatherStats scatterGatherStats, @Nullable Boolean isOfflineTable,
      @Nonnull BrokerMetrics brokerMetrics) throws InterruptedException {
    return sendRequest(new ScatterGatherRequestContext(scatterGatherRequest), scatterGatherStats, isOfflineTable,
        brokerMetrics);
  }

  @Nonnull
  @Override
  public CompositeFuture<byte[]> scatterGather(@Nonnull ScatterGatherRequest scatterGatherRequest,
      @Nonnull ScatterGatherStats scatterGatherStats, @Nonnull BrokerMetrics brokerMetrics)
      throws InterruptedException {
    return scatterGather(scatterGatherRequest, scatterGatherStats, null, brokerMetrics);
  }

  /**
   *
   * Helper Function to send scatter-request. This method should be called after the servers are selected
   *
   * @param scatterGatherRequestContext Scatter-Gather Request context with selected servers for each request.
   * @param scatterGatherStats scatter-gather statistics.
   * @param isOfflineTable whether the scatter-gather target is an OFFLINE table.
   * @param brokerMetrics broker metrics to track execution statistics.
   * @return a composite future representing the gather process.
   * @throws InterruptedException
   */
  private CompositeFuture<byte[]> sendRequest(ScatterGatherRequestContext scatterGatherRequestContext,
      ScatterGatherStats scatterGatherStats, Boolean isOfflineTable, BrokerMetrics brokerMetrics)
      throws InterruptedException {
    ScatterGatherRequest scatterGatherRequest = scatterGatherRequestContext._request;
    Map<String, List<String>> routingTable = scatterGatherRequest.getRoutingTable();
    CountDownLatch requestDispatchLatch = new CountDownLatch(routingTable.size());

    // async checkout of connections and then dispatch of request
    List<SingleRequestHandler> handlers = new ArrayList<>(routingTable.size());

    for (Entry<String, List<String>> entry : routingTable.entrySet()) {
      ServerInstance serverInstance = ServerInstance.forInstanceName(entry.getKey());
      String shortServerName = serverInstance.getShortHostName();
      if (isOfflineTable != null) {
        if (isOfflineTable) {
          shortServerName += ScatterGatherStats.OFFLINE_TABLE_SUFFIX;
        } else {
          shortServerName += ScatterGatherStats.REALTIME_TABLE_SUFFIX;
        }
      }
      scatterGatherStats.initServer(shortServerName);
      SingleRequestHandler handler =
          new SingleRequestHandler(_connPool, serverInstance, scatterGatherRequest, entry.getValue(),
              scatterGatherRequestContext.getRemainingTimeMs(), requestDispatchLatch, brokerMetrics);
      // Submit to thread-pool for checking-out and sending request
      _executorService.submit(handler);
      handlers.add(handler);
    }

    // Create the composite future for returning
    CompositeFuture<byte[]> response = new CompositeFuture<>("scatterRequest " + scatterGatherRequest.getRequestId(),
        GatherModeOnError.SHORTCIRCUIT_AND);

    // Wait for requests to be sent
    long timeRemaining = scatterGatherRequestContext.getRemainingTimeMs();
    boolean sentSuccessfully = requestDispatchLatch.await(timeRemaining, TimeUnit.MILLISECONDS);

    if (sentSuccessfully) {
      List<ServerResponseFuture<byte[]>> responseFutures = new ArrayList<>();
      for (SingleRequestHandler h : handlers) {
        responseFutures.add(h.getResponseFuture());
        String shortServerName = h.getServer().getShortHostName();
        if (isOfflineTable != null) {
          if (isOfflineTable) {
            shortServerName += ScatterGatherStats.OFFLINE_TABLE_SUFFIX;
          } else {
            shortServerName += ScatterGatherStats.REALTIME_TABLE_SUFFIX;
          }
        }
        scatterGatherStats.setSendStartTimeMillis(shortServerName, h.getConnStartTimeMillis());
        scatterGatherStats.setConnStartTimeMillis(shortServerName, h.getStartDelayMillis());
        scatterGatherStats.setSendCompletionTimeMillis(shortServerName, h.getSendCompletionTimeMillis());
      }
      response.start(responseFutures);
    } else {
      LOGGER.error(
          "Request (" + scatterGatherRequest.getRequestId() + ") not sent completely within time (" + timeRemaining
              + " ms) !! Cancelling !!. NumSentFailed:" + requestDispatchLatch.getCount());
      response.start(null);

      // Some requests were not event sent (possibly because of checkout !!)
      // and so we cancel all of them here
      for (SingleRequestHandler h : handlers) {
        LOGGER.info("Request {} to {} was sent successfully:{}, cancelling.", scatterGatherRequest.getRequestId(),
            h.getServer(), h.isSent());
        h.cancel();
      }
    }

    return response;
  }

  private static class ScatterGatherRequestContext {
    private final ScatterGatherRequest _request;
    private final long _startTimeMs;

    public ScatterGatherRequestContext(ScatterGatherRequest request) {
      _request = request;
      _startTimeMs = System.currentTimeMillis();
    }

    public long getRemainingTimeMs() {
      long timeout = _request.getRequestTimeoutMs();

      if (timeout < 0) {
        return Long.MAX_VALUE;
      }

      long timeElapsed = System.currentTimeMillis() - _startTimeMs;
      return timeout - timeElapsed;
    }
  }

  /**
   * Runnable responsible for sending a request to the server once the connection is available
   *
   */
  private static class SingleRequestHandler implements Runnable {
    private final static int MAX_CONN_RETRIES = 3;  // Max retries for getting a connection
    // Scatter Request
    private final ScatterGatherRequest _request;
    // List Of Partitions to be queried on the server
    private final List<String> _segments;
    // Server Instance to be queried
    private final ServerInstance _server;
    // Latch to signal completion of dispatching request
    private final CountDownLatch _requestDispatchLatch;
    // Future for the response
    private volatile ResponseFuture _responseFuture;

    // Connection Pool: Used if we need to checkin/destroy object in case of timeout
    private final KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _connPool;

    // Track if request has been dispatched
    private final AtomicBoolean _isSent = new AtomicBoolean(false);

    // Cancel dispatching request
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);

    // Remaining time budget to connect and process the request.
    private final long _timeoutMS;

    private final long _initTime;
    private final BrokerMetrics _brokerMetrics;
    private long _startTime;
    private long _endTime;

    public SingleRequestHandler(KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connPool,
        ServerInstance server, ScatterGatherRequest request, List<String> segments, long timeoutMS,
        CountDownLatch latch, final BrokerMetrics brokerMetrics) {
      _connPool = connPool;
      _server = server;
      _request = request;
      _segments = segments;
      _requestDispatchLatch = latch;
      _timeoutMS = timeoutMS;
      _initTime = System.currentTimeMillis();
      _brokerMetrics = brokerMetrics;
    }

    @Override
    public synchronized void run() {
      try {
        _startTime = System.currentTimeMillis();
        runInternal();
      } finally {
        _endTime = System.currentTimeMillis();
      }
    }

    public long getConnStartTimeMillis() {
      return _startTime - _initTime;
    }

    public long getSendCompletionTimeMillis() {
      return _endTime > _initTime ? _endTime - _initTime : 0;
    }

    // If the 'run' gets called more than 5ms after we created this object, something is wrong.
    public long getStartDelayMillis() {
      return _startTime - _initTime;
    }

    private void runInternal() {
      if (_isCancelled.get()) {
        LOGGER.error("Request {} to server {} cancelled even before request is sent !! Not sending request",
            _request.getRequestId(), _server);
        _requestDispatchLatch.countDown();
        return;
      }

      PooledNettyClientResourceManager.PooledClientConnection conn = null;
      ServerResponseFuture<PooledNettyClientResourceManager.PooledClientConnection> serverResponseFuture = null;
      boolean gotConnection = false;
      boolean error = true;
      long timeRemainingMillis = _timeoutMS - (System.currentTimeMillis() - _startTime);
      long startTimeNs = System.nanoTime();
      long timeWaitedNs = 0;
      try {
        serverResponseFuture = _connPool.checkoutObject(_server, String.valueOf(_request.getRequestId()));

        byte[] serializedRequest = _request.getRequestForService(_segments);
        int ntries = 0;
        // Try a maximum of pool size objects.
        while (true) {
          if (timeRemainingMillis <= 0) {
            throw new TimeoutException(
                "Timed out trying to connect to " + _server + "(timeout=" + _timeoutMS + "ms,ntries=" + ntries + ")");
          }
          conn = serverResponseFuture.getOne(timeRemainingMillis, TimeUnit.MILLISECONDS);
          if (conn != null && conn.validate()) {
            timeWaitedNs = System.nanoTime() - startTimeNs;
            gotConnection = true;
            break;
          }
          // conn may be null when the AsyncPoolImpl.create()
          // is not able to create a connection, and this tried to wait for a connection, and it reached the max waiters limit.
          // In that case, there is no point in retrying  this request
          // If we get a null error map, then it is most likely a case of max waiters limit.
          // The connect errors are obtained from two different objects -- 'conn' and 'keyedFuture'.
          // We pick the error from 'keyedFuture' here, if we find it. Unfortunately there is not a way (now) to pass the
          // error from 'keyedFuture' to 'conn' (need to do it via AsyncPoolImpl)
          Map<ServerInstance, Throwable> errorMap = serverResponseFuture.getError();
          String errStr = "";
          if (errorMap != null && errorMap.containsKey(_server)) {
            errStr = errorMap.get(_server).getMessage();
          }
          if (conn != null) {
            LOGGER.warn("Destroying invalid conn {}:{}", conn, errStr);
            _connPool.destroyObject(_server, conn);
          }
          if (++ntries == MAX_CONN_RETRIES - 1) {
            throw new ConnectionLimitReachedException(
                "Could not connect to " + _server + " after " + ntries + " attempts(timeRemaining="
                    + timeRemainingMillis + "ms)");
          }
          serverResponseFuture = _connPool.checkoutObject(_server, "none");
          timeRemainingMillis = _timeoutMS - (System.currentTimeMillis() - _startTime);
        }
        ByteBuf req = Unpooled.wrappedBuffer(serializedRequest);
        _responseFuture = conn.sendRequest(req, _request.getRequestId(), timeRemainingMillis);
        _isSent.set(true);
        LOGGER.debug("Response Future is : {}", _responseFuture);
        error = false;
      } catch (TimeoutException e1) {
        LOGGER.warn("Timed out waiting for connection for server ({})({})(gotConnection={}):{}. See metric {}", _server,
            _request.getRequestId(), gotConnection, e1.getMessage(),
            BrokerMeter.REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR.getMeterName());
        _responseFuture = new ResponseFuture(_server, e1, "Error Future for request " + _request.getRequestId());
      } catch (ConnectionLimitReachedException e) {
        LOGGER.warn("Request {} not sent (gotConnection={}):{}. See metric {}", _request.getRequestId(), gotConnection,
            e.getMessage(), BrokerMeter.REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR.getMeterName());
        _responseFuture = new ResponseFuture(_server, e, "Error Future for request " + _request.getRequestId());
      } catch (Exception e) {
        LOGGER.error("Got exception sending request ({})(gotConnection={}). Setting error future",
            _request.getRequestId(), gotConnection, e);
        _responseFuture = new ResponseFuture(_server, e, "Error Future for request " + _request.getRequestId());
      } finally {
        _requestDispatchLatch.countDown();
        BrokerRequest brokerRequest = _request.getBrokerRequest();
        _brokerMetrics.addPhaseTiming(brokerRequest, BrokerQueryPhase.REQUEST_CONNECTION_WAIT, timeWaitedNs);
        if (timeRemainingMillis < 0) {
          _brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.REQUEST_CONNECTION_TIMEOUTS, 1);
        }
        if (error) {
          if (gotConnection) {
            // We must have failed sometime when sending the request
            _brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.REQUEST_DROPPED_DUE_TO_SEND_ERROR, 1);
          } else {
            // If we got a keyed future, but we did not get to send a request, then cancel the future.
            if (serverResponseFuture != null) {
              serverResponseFuture.cancel(true);
            }
            _brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR, 1);
          }
        }
      }
    }

    /**
     * Cancel checking-out request if possible. If in unsafe state (request already sent),
     * discard the connection from the pool.
     */
    public synchronized void cancel() {
      if (_isCancelled.get()) {
        return;
      }

      _isCancelled.set(true);

      if (_isSent.get()) {
        /**
         * If the request has already been sent, we cancel the
         * response future. The connection will automatically be returned to the pool if response
         * arrived within timeout or discarded if timeout happened. No need to handle it here.
         */
        _responseFuture.cancel(true);
      }
    }

    public ServerInstance getServer() {
      return _server;
    }

    public ResponseFuture getResponseFuture() {
      return _responseFuture;
    }

    public boolean isSent() {
      return _isSent.get();
    }
  }

  private static class ConnectionLimitReachedException extends RuntimeException {
    ConnectionLimitReachedException(String msg) {
      super(msg);
    }
  }
}
