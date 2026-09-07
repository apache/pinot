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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
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
@ThreadSafe
public abstract class BaseExponentialBackoffRetryFailureDetector implements FailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExponentialBackoffRetryFailureDetector.class);

  /// How long a consecutive-timeout count stays valid. Well beyond any sane query timeout, but short enough that
  /// counts do not survive an idle period or a rolling restart and eject a server on its first miss afterwards.
  private static final long TIMEOUT_COUNT_EXPIRY_NS = TimeUnit.MINUTES.toNanos(5);

  protected final String _name = getClass().getSimpleName();
  protected final ConcurrentHashMap<String, RetryInfo> _unhealthyServerRetryInfoMap = new ConcurrentHashMap<>();
  protected final DelayQueue<RetryInfo> _retryInfoDelayQueue = new DelayQueue<>();

  /// Per-server count of consecutive requests left unanswered, keyed by instance id. An entry is dropped as soon as
  /// the server answers anything, and a count older than [#TIMEOUT_COUNT_EXPIRY_NS] restarts from one so that
  /// "consecutive" stays consecutive in time and not merely in arrival order.
  protected final ConcurrentHashMap<String, TimeoutCount> _consecutiveTimeoutCountMap = new ConcurrentHashMap<>();

  protected final List<Function<String, ServerState>> _unhealthyServerRetriers = new ArrayList<>();
  protected Consumer<String> _healthyServerNotifier;
  protected Consumer<String> _unhealthyServerNotifier;
  protected BrokerMetrics _brokerMetrics;
  protected long _retryInitialDelayNs;
  protected double _retryDelayFactor;
  protected int _maxRetries;
  protected int _consecutiveTimeoutThreshold;
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
    _consecutiveTimeoutThreshold = config.getProperty(Broker.FailureDetector.CONFIG_OF_CONSECUTIVE_TIMEOUT_THRESHOLD,
        Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD);
    LOGGER.info("Initialized {} with retry initial delay: {}ms, exponential backoff factor: {}, max retries: {}, "
            + "consecutive timeout threshold: {}", _name, retryInitialDelayMs, _retryDelayFactor, _maxRetries,
        _consecutiveTimeoutThreshold);
  }

  @Override
  public void registerUnhealthyServerRetrier(Function<String, ServerState> unhealthyServerRetrier) {
    _unhealthyServerRetriers.add(unhealthyServerRetrier);
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
          LOGGER.info("Retry unhealthy server: {}", instanceId);
          boolean recovered = true;
          for (Function<String, ServerState> unhealthyServerRetrier : _unhealthyServerRetriers) {
            ServerState serverState = unhealthyServerRetrier.apply(instanceId);
            if (serverState == ServerState.UNHEALTHY) {
              recovered = false;
              break;
            }
          }
          if (recovered) {
            markServerHealthy(instanceId, retryInfo._hostName);
          } else {
            // Update the retry info and add it back to the delay queue
            retryInfo._retryDelayNs = (long) (retryInfo._retryDelayNs * _retryDelayFactor);
            retryInfo._retryTimeNs = System.nanoTime() + retryInfo._retryDelayNs;
            retryInfo._numRetries++;
            _retryInfoDelayQueue.offer(retryInfo);
          }
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

  @Override
  public void markServerHealthy(String instanceId, @Nullable String hostName) {
    _consecutiveTimeoutCountMap.remove(instanceId);
    _unhealthyServerRetryInfoMap.computeIfPresent(instanceId, (id, retryInfo) -> {
      LOGGER.info("Mark server: {} {} as healthy", instanceId, hostName);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() - 1);
      _healthyServerNotifier.accept(instanceId);
      return null;
    });
  }

  @Override
  public void markServerUnhealthy(String instanceId, @Nullable String hostName) {
    _unhealthyServerRetryInfoMap.computeIfAbsent(instanceId, id -> {
      LOGGER.warn("Mark server: {} {} as unhealthy", instanceId, hostName);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() + 1);
      _unhealthyServerNotifier.accept(instanceId);
      RetryInfo retryInfo = new RetryInfo(id, hostName);
      _retryInfoDelayQueue.offer(retryInfo);
      return retryInfo;
    });
  }

  @Override
  public void notifyServerResponded(String instanceId) {
    _consecutiveTimeoutCountMap.remove(instanceId);
  }

  /// {@inheritDoc}
  ///
  /// Marks the server unhealthy once it has left
  /// [Broker.FailureDetector#CONFIG_OF_CONSECUTIVE_TIMEOUT_THRESHOLD] consecutive requests unanswered. Any number of
  /// servers may be held out this way: given the caller reports only sole non-responders, a shared cause accuses
  /// nobody, while servers going silent independently should all come out.
  @Override
  public void notifyServerNotResponded(String instanceId, @Nullable String hostName) {
    if (_consecutiveTimeoutThreshold <= 0) {
      return;
    }
    long nowNs = System.nanoTime();
    // compute() applies the whole update under the map's per-key lock, so it cannot interleave with the remove() in
    // notifyServerResponded and drop a reset on the floor.
    TimeoutCount timeoutCount = _consecutiveTimeoutCountMap.compute(instanceId,
        (id, current) -> current == null || nowNs - current._lastTimeoutNs > TIMEOUT_COUNT_EXPIRY_NS
            ? new TimeoutCount(1, nowNs) : new TimeoutCount(current._count + 1, nowNs));
    // Only act on the crossing: past it the server is already unhealthy, so re-marking would just re-log every query.
    if (timeoutCount._count != _consecutiveTimeoutThreshold) {
      return;
    }
    LOGGER.warn("Server: {} {} left {} consecutive requests unanswered while staying connected, marking it unhealthy",
        instanceId, hostName, timeoutCount._count);
    markServerUnhealthy(instanceId, hostName);
  }

  @Override
  public Set<String> getUnhealthyServers() {
    return _unhealthyServerRetryInfoMap.keySet();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}", _name);
    _running = false;

    try {
      _retryThread.interrupt();
      _retryThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for retry thread to finish", e);
    }
  }

  /// A count of consecutive unanswered requests, with the time of the last one so that the count can expire.
  /// Not a record: pinot-common still targets Java 11 bytecode.
  private static class TimeoutCount {
    final int _count;
    final long _lastTimeoutNs;

    TimeoutCount(int count, long lastTimeoutNs) {
      _count = count;
      _lastTimeoutNs = lastTimeoutNs;
    }
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
