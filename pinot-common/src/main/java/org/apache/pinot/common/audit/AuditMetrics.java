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
package org.apache.pinot.common.audit;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.spi.services.ServiceRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


/**
 * Abstraction layer for audit metrics that handles the complexity of routing metrics
 * to the appropriate component-specific metrics (Controller vs Broker).
 * This class provides a clean API for audit components to record metrics without
 * needing to know about the underlying component type.
 */
@Singleton
public class AuditMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(AuditMetrics.class);

  private final TimerRecorder _timerRecorder;
  private final MeterRecorder _meterRecorder;

  /**
   * Creates an AuditMetrics instance that will route metrics to the appropriate component.
   *
   * @param delegate The component-specific metrics instance (ControllerMetrics or BrokerMetrics)
   * @param serviceRole The service role (CONTROLLER, BROKER, etc.)
   */
  @Inject
  public AuditMetrics(AbstractMetrics<?, ?, ?, ?> delegate, ServiceRole serviceRole) {
    requireNonNull(delegate, "Component metrics cannot be null");
    requireNonNull(serviceRole, "Service role cannot be null");

    // One-time setup of delegation logic based on service role
    switch (serviceRole) {
      case CONTROLLER:
        final ControllerMetrics controllerMetrics = (ControllerMetrics) delegate;
        _timerRecorder = (timer, durationMs) -> controllerMetrics.addTimedValue(timer.getControllerTimer(), durationMs,
            TimeUnit.MILLISECONDS);
        _meterRecorder = (meter, count) -> controllerMetrics.addMeteredGlobalValue(meter.getControllerMeter(), count);
        break;
      case BROKER:
        final BrokerMetrics brokerMetrics = (BrokerMetrics) delegate;
        _timerRecorder = (timer, durationMs) -> brokerMetrics.addTimedValue(timer.getBrokerTimer(), durationMs,
            TimeUnit.MILLISECONDS);
        _meterRecorder = (meter, count) -> brokerMetrics.addMeteredGlobalValue(meter.getBrokerMeter(), count);
        break;
      default:
        throw new IllegalArgumentException("Audit not supported for service role: " + serviceRole);
    }
  }

  /**
   * Adds a timed value for the given audit timer.
   * This method mirrors the AbstractMetrics.addTimedValue() interface.
   *
   * @param timer The audit timer to record
   * @param duration The duration to record
   */
  public void addTimedValue(AuditTimer timer, long duration) {
    _timerRecorder.record(timer, duration);
  }

  /**
   * Adds a metered global value for the given audit meter.
   * This method mirrors the AbstractMetrics.addMeteredGlobalValue() interface.
   *
   * @param meter The audit meter to record
   * @param count The count to record
   */
  public void addMeteredGlobalValue(AuditMeter meter, long count) {
    _meterRecorder.record(meter, count);
  }

  public enum AuditTimer {
    AUDIT_REQUEST_PROCESSING_TIME(ControllerTimer.AUDIT_REQUEST_PROCESSING_TIME,
        BrokerTimer.AUDIT_REQUEST_PROCESSING_TIME),
    AUDIT_RESPONSE_PROCESSING_TIME(ControllerTimer.AUDIT_RESPONSE_PROCESSING_TIME,
        BrokerTimer.AUDIT_RESPONSE_PROCESSING_TIME);

    final ControllerTimer _controllerTimer;
    private final BrokerTimer _brokerTimer;

    AuditTimer(ControllerTimer controllerTimer, BrokerTimer brokerTimer) {
      _controllerTimer = controllerTimer;
      _brokerTimer = brokerTimer;
    }

    public ControllerTimer getControllerTimer() {
      return _controllerTimer;
    }

    public BrokerTimer getBrokerTimer() {
      return _brokerTimer;
    }
  }

  public enum AuditMeter {
    AUDIT_REQUEST_FAILURES(ControllerMeter.AUDIT_REQUEST_FAILURES, BrokerMeter.AUDIT_REQUEST_FAILURES),
    AUDIT_RESPONSE_FAILURES(ControllerMeter.AUDIT_RESPONSE_FAILURES, BrokerMeter.AUDIT_RESPONSE_FAILURES),
    AUDIT_REQUEST_PAYLOAD_TRUNCATED(ControllerMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED,
        BrokerMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED);

    private final ControllerMeter _controllerMeter;
    private final BrokerMeter _brokerMeter;

    AuditMeter(ControllerMeter controllerMeter, BrokerMeter brokerMeter) {
      _controllerMeter = controllerMeter;
      _brokerMeter = brokerMeter;
    }

    public ControllerMeter getControllerMeter() {
      return _controllerMeter;
    }

    public BrokerMeter getBrokerMeter() {
      return _brokerMeter;
    }
  }

  // Functional interfaces for type-safe delegation
  @FunctionalInterface
  interface TimerRecorder {
    void record(AuditTimer timer, long durationMs);
  }

  @FunctionalInterface
  interface MeterRecorder {
    void record(AuditMeter meter, long count);
  }
}
