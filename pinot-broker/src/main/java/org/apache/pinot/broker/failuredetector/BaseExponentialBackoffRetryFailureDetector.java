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
package org.apache.pinot.broker.failuredetector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
  protected final Logger _logger = LoggerFactory.getLogger(getClass());
  protected final String _name = getClass().getSimpleName();
  protected final List<Listener> _listeners = new ArrayList<>();
  protected final ConcurrentHashMap<String, RetryInfo> _unhealthyServerRetryInfoMap = new ConcurrentHashMap<>();

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
        config.getProperty(Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, Broker.FailureDetector.DEFAULT_MAX_RETIRES);
    _logger.info("Initialized {} with retry initial delay: {}ms, exponential backoff factor: {}, max " + "retries: {}",
        _name, retryInitialDelayMs, _retryDelayFactor, _maxRetries);
  }

  @Override
  public void register(Listener listener) {
    _listeners.add(listener);
  }

  @Override
  public void start() {
    _logger.info("Starting {}", _name);
    _running = true;

    _retryThread = new Thread(() -> {
      while (_running) {
        try {
          long earliestRetryTimeNs = System.nanoTime() + _retryInitialDelayNs;
          for (Map.Entry<String, RetryInfo> entry : _unhealthyServerRetryInfoMap.entrySet()) {
            RetryInfo retryInfo = entry.getValue();
            if (System.nanoTime() > retryInfo._retryTimeNs) {
              String instanceId = entry.getKey();
              if (retryInfo._numRetires == _maxRetries) {
                _logger.warn(
                    "Unhealthy server: {} already reaches the max retries: {}, do not retry again and treat it as "
                        + "healthy so that the listeners do not lose track of the server", instanceId, _maxRetries);
                markServerHealthy(instanceId);
                continue;
              }
              _logger.info("Retry unhealthy server: {}", instanceId);
              for (Listener listener : _listeners) {
                listener.retryUnhealthyServer(instanceId, this);
              }
              // Update the retry info
              retryInfo._retryDelayNs = (long) (retryInfo._retryDelayNs * _retryDelayFactor);
              retryInfo._retryTimeNs = System.nanoTime() + retryInfo._retryDelayNs;
              retryInfo._numRetires++;
            } else {
              earliestRetryTimeNs = Math.min(earliestRetryTimeNs, retryInfo._retryTimeNs);
            }
          }
          //noinspection BusyWait
          Thread.sleep(TimeUnit.NANOSECONDS.toMillis(earliestRetryTimeNs - System.nanoTime()));
        } catch (Exception e) {
          if (_running) {
            _logger.error("Caught exception in the retry thread, continuing with errors", e);
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
    if (_unhealthyServerRetryInfoMap.remove(instanceId) != null) {
      _logger.info("Mark server: {} as healthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size());
      for (Listener listener : _listeners) {
        listener.notifyHealthyServer(instanceId, this);
      }
    }
  }

  @Override
  public void markServerUnhealthy(String instanceId) {
    if (_unhealthyServerRetryInfoMap.putIfAbsent(instanceId, new RetryInfo()) == null) {
      _logger.warn("Mark server: {} as unhealthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size());
      for (Listener listener : _listeners) {
        listener.notifyUnhealthyServer(instanceId, this);
      }
    }
  }

  @Override
  public Set<String> getUnhealthyServers() {
    return _unhealthyServerRetryInfoMap.keySet();
  }

  @Override
  public void stop() {
    _logger.info("Stopping {}", _name);
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
  protected class RetryInfo {
    long _retryTimeNs;
    long _retryDelayNs;
    int _numRetires;

    RetryInfo() {
      _retryTimeNs = System.nanoTime() + _retryInitialDelayNs;
      _retryDelayNs = _retryInitialDelayNs;
      _numRetires = 0;
    }
  }
}
