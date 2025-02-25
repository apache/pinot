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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code BaseExponentialBackoffRetryFailureDetector} is a base failure detector implementation that retries the
 * unhealthy servers with exponential increasing delays.
 */
@ThreadSafe
public abstract class BaseExponentialBackoffRetryFailureDetector implements FailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExponentialBackoffRetryFailureDetector.class);

  protected final String _name = getClass().getSimpleName();
  protected final ConcurrentHashMap<String, RetryInfo> _unhealthyServerRetryInfoMap = new ConcurrentHashMap<>();
  protected final DelayQueue<RetryInfo> _retryInfoDelayQueue = new DelayQueue<>();

  protected final List<Function<String, ServerState>> _unhealthyServerRetriers = new ArrayList<>();
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
            markServerHealthy(instanceId);
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
            markServerHealthy(instanceId);
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
  public void markServerHealthy(String instanceId) {
    _unhealthyServerRetryInfoMap.computeIfPresent(instanceId, (id, retryInfo) -> {
      LOGGER.info("Mark server: {} as healthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() - 1);
      _healthyServerNotifier.accept(instanceId);
      return null;
    });
  }

  @Override
  public void markServerUnhealthy(String instanceId) {
    _unhealthyServerRetryInfoMap.computeIfAbsent(instanceId, id -> {
      LOGGER.warn("Mark server: {} as unhealthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() + 1);
      _unhealthyServerNotifier.accept(instanceId);
      RetryInfo retryInfo = new RetryInfo(id);
      _retryInfoDelayQueue.offer(retryInfo);
      return retryInfo;
    });
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

  /**
   * Encapsulates the retry related information.
   */
  protected class RetryInfo implements Delayed {
    final String _instanceId;

    long _retryTimeNs;
    long _retryDelayNs;
    int _numRetries;

    RetryInfo(String instanceId) {
      _instanceId = instanceId;
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
