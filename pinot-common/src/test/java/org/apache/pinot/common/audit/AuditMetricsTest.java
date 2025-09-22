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
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.spi.services.ServiceRole;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;


/**
 * Unit tests for {@link AuditMetrics} delegation mechanism.
 *
 * Tests verify that AuditMetrics correctly routes metrics calls to the appropriate
 * component-specific metrics (Controller vs Broker) based on service role.
 */
public class AuditMetricsTest {

  @Mock
  private ControllerMetrics _controllerMetrics;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testControllerTimerDelegation() {
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.CONTROLLER);

    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 150L);

    verify(_controllerMetrics).addTimedValue(ControllerTimer.AUDIT_REQUEST_PROCESSING_TIME, 150L,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void testControllerMeterDelegation() {
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.CONTROLLER);

    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES, 3L);

    verify(_controllerMetrics).addMeteredGlobalValue(ControllerMeter.AUDIT_REQUEST_FAILURES, 3L);
  }

  @Test
  public void testBrokerTimerDelegation() {
    AuditMetrics auditMetrics = new AuditMetrics(_brokerMetrics, ServiceRole.BROKER);

    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_RESPONSE_PROCESSING_TIME, 75L);

    verify(_brokerMetrics).addTimedValue(BrokerTimer.AUDIT_RESPONSE_PROCESSING_TIME, 75L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testBrokerMeterDelegation() {
    AuditMetrics auditMetrics = new AuditMetrics(_brokerMetrics, ServiceRole.BROKER);

    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_RESPONSE_FAILURES, 1L);

    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.AUDIT_RESPONSE_FAILURES, 1L);
  }

  @Test
  public void testAllControllerTimers() {
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.CONTROLLER);

    // Test all audit timer mappings
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L);
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_RESPONSE_PROCESSING_TIME, 200L);

    verify(_controllerMetrics).addTimedValue(ControllerTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L,
        TimeUnit.MILLISECONDS);
    verify(_controllerMetrics).addTimedValue(ControllerTimer.AUDIT_RESPONSE_PROCESSING_TIME, 200L,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void testAllControllerMeters() {
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.CONTROLLER);

    // Test all audit meter mappings
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES, 1L);
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_RESPONSE_FAILURES, 2L);
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED, 3L);

    verify(_controllerMetrics).addMeteredGlobalValue(ControllerMeter.AUDIT_REQUEST_FAILURES, 1L);
    verify(_controllerMetrics).addMeteredGlobalValue(ControllerMeter.AUDIT_RESPONSE_FAILURES, 2L);
    verify(_controllerMetrics).addMeteredGlobalValue(ControllerMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED, 3L);
  }

  @Test
  public void testAllBrokerTimers() {
    AuditMetrics auditMetrics = new AuditMetrics(_brokerMetrics, ServiceRole.BROKER);

    // Test all audit timer mappings
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 50L);
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_RESPONSE_PROCESSING_TIME, 25L);

    verify(_brokerMetrics).addTimedValue(BrokerTimer.AUDIT_REQUEST_PROCESSING_TIME, 50L, TimeUnit.MILLISECONDS);
    verify(_brokerMetrics).addTimedValue(BrokerTimer.AUDIT_RESPONSE_PROCESSING_TIME, 25L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testAllBrokerMeters() {
    AuditMetrics auditMetrics = new AuditMetrics(_brokerMetrics, ServiceRole.BROKER);

    // Test all audit meter mappings
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES, 5L);
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_RESPONSE_FAILURES, 10L);
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED, 15L);

    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.AUDIT_REQUEST_FAILURES, 5L);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.AUDIT_RESPONSE_FAILURES, 10L);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.AUDIT_REQUEST_PAYLOAD_TRUNCATED, 15L);
  }

  @Test
  public void testUnsupportedServiceRoleNoOp() {
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.MINION);

    // These calls should be no-ops and not throw exceptions
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L);
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES, 1L);

    // Verify no interactions with the mock since unsupported roles use no-op recorders
    verifyNoInteractions(_controllerMetrics);
  }

  @Test
  public void testNullMetricsThrowsException() {
    assertThatThrownBy(() -> new AuditMetrics(null, ServiceRole.CONTROLLER)).isInstanceOf(NullPointerException.class)
        .hasMessage("Component metrics cannot be null");
  }

  @Test
  public void testNullServiceRoleThrowsException() {
    assertThatThrownBy(() -> new AuditMetrics(_controllerMetrics, null)).isInstanceOf(NullPointerException.class)
        .hasMessage("Service role cannot be null");
  }

  @Test
  public void testEnumMappingsConsistency() {
    // Verify that enum mappings are consistent between timer/meter types
    for (AuditMetrics.AuditTimer timer : AuditMetrics.AuditTimer.values()) {
      // Should not throw any exceptions when accessing underlying timers
      timer.getControllerTimer();
      timer.getBrokerTimer();
    }

    for (AuditMetrics.AuditMeter meter : AuditMetrics.AuditMeter.values()) {
      // Should not throw any exceptions when accessing underlying meters
      meter.getControllerMeter();
      meter.getBrokerMeter();
    }
  }

  @Test
  public void testServiceRoleTypeCasting() {
    // Test that type casting works correctly for each supported service role
    ControllerMetrics controllerMetrics = mock(ControllerMetrics.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);

    // Controller casting
    AuditMetrics controllerAuditMetrics = new AuditMetrics(controllerMetrics, ServiceRole.CONTROLLER);
    controllerAuditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L);
    verify(controllerMetrics).addTimedValue(ControllerTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L, TimeUnit.MILLISECONDS);

    // Broker casting
    AuditMetrics brokerAuditMetrics = new AuditMetrics(brokerMetrics, ServiceRole.BROKER);
    brokerAuditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 200L);
    verify(brokerMetrics).addTimedValue(BrokerTimer.AUDIT_REQUEST_PROCESSING_TIME, 200L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testFunctionalInterfacesExecution() {
    // Test that functional interfaces are properly executed with correct parameters
    AuditMetrics auditMetrics = new AuditMetrics(_controllerMetrics, ServiceRole.CONTROLLER);

    // Test timer functional interface
    auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 500L);
    verify(_controllerMetrics).addTimedValue(ControllerTimer.AUDIT_REQUEST_PROCESSING_TIME, 500L,
        TimeUnit.MILLISECONDS);

    // Test meter functional interface
    auditMetrics.addMeteredGlobalValue(AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES, 10L);
    verify(_controllerMetrics).addMeteredGlobalValue(ControllerMeter.AUDIT_REQUEST_FAILURES, 10L);
  }

  @Test
  public void testErrorHandlingInFunctionalInterfaces() {
    // Simulate error in underlying metrics call
    ControllerMetrics faultyMetrics = mock(ControllerMetrics.class);
    doThrow(new RuntimeException("Metrics system failure")).when(faultyMetrics).addTimedValue(any(), anyLong(), any());

    AuditMetrics auditMetrics = new AuditMetrics(faultyMetrics, ServiceRole.CONTROLLER);

    // This should throw since we're not handling exceptions in the functional interface itself
    assertThatThrownBy(
        () -> auditMetrics.addTimedValue(AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME, 100L)).isInstanceOf(
        RuntimeException.class).hasMessage("Metrics system failure");
  }
}
