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
package org.apache.pinot.common.failuredetector;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `BaseExponentialBackoffRetryFailureDetector` is a base failure detector implementation that retries the
/// unhealthy servers with exponential increasing delays.
///
/// Every check of a server -- a retry, or a check after a query to it timed out -- runs on a small pool rather than on
/// the retry thread, because a check can wait out a ping timeout and one dead server must not hold up the checks of the
/// others. A retry runs the registered retriers; a check after a timeout runs the registered checkers only.
@ThreadSafe
public abstract class BaseExponentialBackoffRetryFailureDetector implements FailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExponentialBackoffRetryFailureDetector.class);
  private static final int MAX_CONCURRENT_CHECKS = 16;

  protected final String _name = getClass().getSimpleName();
  protected final ConcurrentHashMap<String, RetryInfo> _unhealthyServerRetryInfoMap = new ConcurrentHashMap<>();
  protected final DelayQueue<RetryInfo> _retryInfoDelayQueue = new DelayQueue<>();
  /// Servers with a check after a timeout in flight, so a burst of timeouts against one server checks it once.
  protected final Set<String> _serversBeingChecked = ConcurrentHashMap.newKeySet();
  /// Both iterated concurrently by the checks.
  protected final List<Function<String, ServerState>> _unhealthyServerRetriers = new CopyOnWriteArrayList<>();
  protected final List<Function<String, ServerState>> _serverNotRespondedCheckers = new CopyOnWriteArrayList<>();
  protected final ThreadPoolExecutor _checkExecutor = createCheckExecutor();
  private final Object _unhealthyServerGaugeLock = new Object();

  protected Consumer<String> _healthyServerNotifier;
  protected Consumer<String> _unhealthyServerNotifier;
  protected BrokerMetrics _brokerMetrics;
  protected long _retryInitialDelayNs;
  protected double _retryDelayFactor;
  protected int _maxRetries;
  protected Thread _retryThread;

  protected volatile boolean _running;

  @Override
  public void init(PinotConfiguration config, BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
    long retryInitialDelayMs = config.getProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS,
        Broker.FailureDetector.DEFAULT_RETRY_INITIAL_DELAY_MS);
    _retryInitialDelayNs = TimeUnit.MILLISECONDS.toNanos(retryInitialDelayMs);
    _retryDelayFactor = config.getProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR,
        Broker.FailureDetector.DEFAULT_RETRY_DELAY_FACTOR);
    _maxRetries =
        config.getProperty(Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, Broker.FailureDetector.DEFAULT_MAX_RETRIES);
    LOGGER.info("Initialized {} with retry initial delay: {}ms, exponential backoff factor: {}, max retries: {}", _name,
        retryInitialDelayMs, _retryDelayFactor, _maxRetries);
  }

  @Override
  public void registerUnhealthyServerRetrier(Function<String, ServerState> unhealthyServerRetrier) {
    _unhealthyServerRetriers.add(unhealthyServerRetrier);
  }

  @Override
  public void registerServerNotRespondedChecker(Function<String, ServerState> serverNotRespondedChecker) {
    _serverNotRespondedCheckers.add(serverNotRespondedChecker);
  }

  @Override
  public void registerHealthyServerNotifier(Consumer<String> healthyServerNotifier) {
    _healthyServerNotifier = healthyServerNotifier;
  }

  @Override
  public void registerUnhealthyServerNotifier(Consumer<String> unhealthyServerNotifier) {
    _unhealthyServerNotifier = unhealthyServerNotifier;
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}", _name);
    _running = true;

    _retryThread = new Thread(() -> {
      while (_running) {
        try {
          RetryInfo retryInfo = _retryInfoDelayQueue.take();
          String instanceId = retryInfo._instanceId;
          if (_unhealthyServerRetryInfoMap.get(instanceId) != retryInfo) {
            LOGGER.info("Server: {} has been marked healthy, skipping the retry", instanceId);
            continue;
          }
          if (retryInfo._numRetries == _maxRetries) {
            LOGGER.warn("Unhealthy server: {} already reaches the max retries: {}, do not retry again and treat it "
                + "as healthy so that the listeners do not lose track of the server", instanceId, _maxRetries);
            markServerHealthy(instanceId, retryInfo._hostName);
            continue;
          }
          _checkExecutor.execute(() -> retry(retryInfo));
        } catch (Exception e) {
          if (_running) {
            LOGGER.error("Caught exception in the retry thread, continuing with errors", e);
          }
        }
      }
    });
    _retryThread.setName("failure-detector-retry");
    _retryThread.setDaemon(true);
    _retryThread.start();
  }

  /// Retries one unhealthy server: marks it healthy if it checks out, otherwise requeues it with a longer delay. A
  /// retrier that throws counts as a failed check, so the server is retried again rather than stranded as unhealthy.
  private void retry(RetryInfo retryInfo) {
    String instanceId = retryInfo._instanceId;
    LOGGER.info("Retry unhealthy server: {}", instanceId);
    boolean recovered = false;
    try {
      recovered = !isReportedUnhealthy(instanceId, _unhealthyServerRetriers);
    } catch (Exception e) {
      LOGGER.error("Caught exception while retrying unhealthy server: {}, retrying again later", instanceId, e);
    }
    if (recovered) {
      markServerHealthy(instanceId, retryInfo._hostName);
    } else {
      retryInfo._retryDelayNs = (long) (retryInfo._retryDelayNs * _retryDelayFactor);
      retryInfo._retryTimeNs = System.nanoTime() + retryInfo._retryDelayNs;
      retryInfo._numRetries++;
      _retryInfoDelayQueue.offer(retryInfo);
    }
  }

  /// Returns whether any of the given retriers or checkers reports the server unhealthy.
  private static boolean isReportedUnhealthy(String instanceId, List<Function<String, ServerState>> checks) {
    for (Function<String, ServerState> check : checks) {
      if (check.apply(instanceId) == ServerState.UNHEALTHY) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void markServerHealthy(String instanceId, @Nullable String hostName) {
    _unhealthyServerRetryInfoMap.computeIfPresent(instanceId, (id, retryInfo) -> {
      LOGGER.info("Mark server: {} {} as healthy", instanceId, hostName);
      _healthyServerNotifier.accept(instanceId);
      return null;
    });
    updateUnhealthyServerGauge();
  }

  @Override
  public void markServerUnhealthy(String instanceId, @Nullable String hostName) {
    _unhealthyServerRetryInfoMap.computeIfAbsent(instanceId, id -> {
      LOGGER.warn("Mark server: {} {} as unhealthy", instanceId, hostName);
      _unhealthyServerNotifier.accept(instanceId);
      RetryInfo retryInfo = new RetryInfo(id, hostName);
      _retryInfoDelayQueue.offer(retryInfo);
      return retryInfo;
    });
    updateUnhealthyServerGauge();
  }

  /// Sets the gauge from the map's size, rather than adjusting it by one inside the compute: two servers changing state
  /// at once would both read the same size there and leave the gauge off by one. Under the lock the last writer reads
  /// a size that already includes every change made before it.
  private void updateUnhealthyServerGauge() {
    synchronized (_unhealthyServerGaugeLock) {
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size());
    }
  }

  /// {@inheritDoc}
  ///
  /// Checks the server with the registered checkers, off the caller's thread, and marks it unhealthy if any of them
  /// reports it unhealthy. A server already unhealthy, or already being checked, is left alone.
  @Override
  public void notifyServerNotResponded(String instanceId, @Nullable String hostName) {
    if (!_running || _serverNotRespondedCheckers.isEmpty() || _unhealthyServerRetryInfoMap.containsKey(instanceId)
        || !_serversBeingChecked.add(instanceId)) {
      return;
    }
    try {
      _checkExecutor.execute(() -> {
        try {
          // Skip marking once stopping: shutdownNow() interrupts the check, which then looks like a failed one.
          if (isReportedUnhealthy(instanceId, _serverNotRespondedCheckers) && _running) {
            LOGGER.warn("Server: {} {} left a query unanswered and failed the follow-up check", instanceId, hostName);
            markServerUnhealthy(instanceId, hostName);
          }
        } catch (Exception e) {
          LOGGER.error("Caught exception while checking server: {} after a query timed out", instanceId, e);
        } finally {
          _serversBeingChecked.remove(instanceId);
        }
      });
    } catch (RejectedExecutionException e) {
      // Only when stopping.
      _serversBeingChecked.remove(instanceId);
    }
  }

  @Override
  public Set<String> getUnhealthyServers() {
    return _unhealthyServerRetryInfoMap.keySet();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}", _name);
    _running = false;
    _checkExecutor.shutdownNow();

    try {
      _retryThread.interrupt();
      _retryThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for retry thread to finish", e);
    }
  }

  /// Bounded, with its threads created on demand and reclaimed when idle, so an idle broker holds none.
  private static ThreadPoolExecutor createCheckExecutor() {
    AtomicInteger threadIndex = new AtomicInteger();
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(MAX_CONCURRENT_CHECKS, MAX_CONCURRENT_CHECKS, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), runnable -> {
          Thread thread = new Thread(runnable, "failure-detector-check-" + threadIndex.getAndIncrement());
          thread.setDaemon(true);
          return thread;
        });
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /// Encapsulates the retry related information.
  protected class RetryInfo implements Delayed {
    final String _instanceId;
    final String _hostName;

    long _retryTimeNs;
    long _retryDelayNs;
    int _numRetries;

    RetryInfo(String instanceId, String hostName) {
      _instanceId = instanceId;
      _hostName = hostName;
      _retryTimeNs = System.nanoTime() + _retryInitialDelayNs;
      _retryDelayNs = _retryInitialDelayNs;
      _numRetries = 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(_retryTimeNs - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      RetryInfo that = (RetryInfo) o;
      return Long.compare(_retryTimeNs, that._retryTimeNs);
    }
  }
}
