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

import java.io.IOException;
import java.util.UUID;
import javax.inject.Provider;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.UriInfo;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link AuditLogFilter} focusing on response auditing feature.
 */
public class AuditLogFilterTest {

  @Mock
  private Provider<Request> _requestProvider;

  @Mock
  private Request _request;

  @Mock
  private AuditRequestProcessor _auditRequestProcessor;

  @Mock
  private AuditConfigManager _configManager;

  @Mock
  private ContainerRequestContext _requestContext;

  @Mock
  private ContainerResponseContext _responseContext;

  @Mock
  private UriInfo _uriInfo;

  private AuditLogFilter _auditLogFilter;
  private MockedStatic<AuditLogger> _auditLoggerMock;
  private AuditConfig _config;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _auditLogFilter =
        new AuditLogFilter(_requestProvider, _auditRequestProcessor, _configManager, mock(AuditMetrics.class));
    _auditLoggerMock = mockStatic(AuditLogger.class);

    _config = new AuditConfig();
    _config.setEnabled(true);
    _config.setCaptureResponseEnabled(false);

    when(_requestProvider.get()).thenReturn(_request);
    when(_request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(_configManager.getCurrentConfig()).thenReturn(_config);
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/api/test");
    when(_requestContext.getMethod()).thenReturn("GET");
  }

  @AfterMethod
  public void tearDown() {
    _auditLoggerMock.close();
  }

  @Test
  public void testResponseAuditingWhenEnabled() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    when(_responseContext.getStatus()).thenReturn(200);

    AuditEvent requestEvent = new AuditEvent();
    when(_auditRequestProcessor.processRequest(any(), anyString())).thenReturn(requestEvent);

    // When - request filter
    _auditLogFilter.filter(_requestContext);

    // Capture the context that was set
    ArgumentCaptor<AuditResponseContext> contextCaptor = ArgumentCaptor.forClass(AuditResponseContext.class);
    verify(_requestContext).setProperty(eq("audit.response.context"), contextCaptor.capture());
    AuditResponseContext capturedContext = contextCaptor.getValue();

    // Simulate time passing
    Thread.yield();

    // Set up the context retrieval for response filter
    when(_requestContext.getProperty("audit.response.context")).thenReturn(capturedContext);

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then
    ArgumentCaptor<AuditEvent> eventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(eventCaptor.capture()), times(2));

    // Verify request audit event has request ID
    AuditEvent capturedRequestEvent = eventCaptor.getAllValues().get(0);
    assertThat(capturedRequestEvent.getRequestId()).isNotNull();
    assertThat(capturedRequestEvent.getRequestId()).matches(
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    // Verify response audit event
    AuditEvent capturedResponseEvent = eventCaptor.getAllValues().get(1);
    assertThat(capturedResponseEvent.getRequestId()).isEqualTo(capturedRequestEvent.getRequestId());
    assertThat(capturedResponseEvent.getResponseCode()).isEqualTo(200);
    assertThat(capturedResponseEvent.getDurationMs()).isNotNull();
    assertThat(capturedResponseEvent.getDurationMs()).isGreaterThanOrEqualTo(0L);
    assertThat(capturedResponseEvent.getEndpoint()).isEqualTo("/api/test");
    assertThat(capturedResponseEvent.getMethod()).isEqualTo("GET");
  }

  @Test
  public void testRequestResponseIdCorrelation() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    when(_responseContext.getStatus()).thenReturn(201);

    AuditEvent requestEvent = new AuditEvent();
    when(_auditRequestProcessor.processRequest(any(), anyString())).thenReturn(requestEvent);

    // When - request filter
    _auditLogFilter.filter(_requestContext);

    // Capture the context
    ArgumentCaptor<AuditResponseContext> contextCaptor = ArgumentCaptor.forClass(AuditResponseContext.class);
    verify(_requestContext).setProperty(eq("audit.response.context"), contextCaptor.capture());
    AuditResponseContext capturedContext = contextCaptor.getValue();

    String requestId = capturedContext.getRequestId();
    assertThat(requestId).isNotNull();
    assertThat(UUID.fromString(requestId)).isNotNull(); // Validates UUID format

    // Set up the context retrieval
    when(_requestContext.getProperty("audit.response.context")).thenReturn(capturedContext);

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then
    ArgumentCaptor<AuditEvent> eventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(eventCaptor.capture()), times(2));

    // Both events should have the same request ID
    String requestEventId = eventCaptor.getAllValues().get(0).getRequestId();
    String responseEventId = eventCaptor.getAllValues().get(1).getRequestId();
    assertThat(requestEventId).isEqualTo(responseEventId);
    assertThat(requestEventId).isEqualTo(requestId);
  }

  @Test
  public void testResponseAuditingDisabledByConfig() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(false);
    when(_responseContext.getStatus()).thenReturn(200);

    // When - request filter
    _auditLogFilter.filter(_requestContext);

    // Then - no context should be set
    verify(_requestContext, never()).setProperty(eq("audit.response.context"), any());

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - no response audit should occur
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), never());
  }

  @Test
  public void testResponseAuditingWhenMainAuditDisabled() throws IOException {
    // Given
    _config.setEnabled(false);
    _config.setCaptureResponseEnabled(true);

    // When - request filter
    _auditLogFilter.filter(_requestContext);

    // Then - no context should be set
    verify(_requestContext, never()).setProperty(anyString(), any());

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - no auditing should occur
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), never());
  }

  @Test
  public void testContextPropagationBetweenFilters() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    when(_responseContext.getStatus()).thenReturn(200);

    // When - request filter
    _auditLogFilter.filter(_requestContext);

    // Capture the context that was stored
    ArgumentCaptor<AuditResponseContext> contextCaptor = ArgumentCaptor.forClass(AuditResponseContext.class);
    verify(_requestContext).setProperty(eq("audit.response.context"), contextCaptor.capture());

    AuditResponseContext storedContext = contextCaptor.getValue();
    assertThat(storedContext).isNotNull();
    assertThat(storedContext.getRequestId()).isNotNull();
    assertThat(storedContext.getStartTimeNanos()).isGreaterThan(0);

    // Set up retrieval
    when(_requestContext.getProperty("audit.response.context")).thenReturn(storedContext);

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - verify the same context was used
    ArgumentCaptor<AuditEvent> eventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(eventCaptor.capture()), atLeastOnce());

    // Find the response event (last one)
    AuditEvent responseEvent = eventCaptor.getAllValues().get(eventCaptor.getAllValues().size() - 1);
    assertThat(responseEvent.getRequestId()).isEqualTo(storedContext.getRequestId());
  }

  @Test
  public void testResponseFilterWithMissingContext() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    when(_requestContext.getProperty("audit.response.context")).thenReturn(null);

    // When - response filter without prior request filter
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - should handle gracefully without throwing exception
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), never());
  }

  @Test
  public void testResponseFilterWithNullRequestId() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    AuditResponseContext contextWithNullId = new AuditResponseContext()
        .setRequestId(null)
        .setStartTimeNanos(System.nanoTime());
    when(_requestContext.getProperty("audit.response.context")).thenReturn(contextWithNullId);

    // When
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - should handle gracefully
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), never());
  }

  @Test
  public void testErrorHandlingInResponseFilter() throws IOException {
    // Given
    _config.setCaptureResponseEnabled(true);
    AuditResponseContext context = new AuditResponseContext()
        .setRequestId(UUID.randomUUID().toString())
        .setStartTimeNanos(System.nanoTime());
    when(_requestContext.getProperty("audit.response.context")).thenReturn(context);

    // Make UriInfo throw exception
    when(_requestContext.getUriInfo()).thenThrow(new RuntimeException("Test exception"));

    // When
    assertThatCode(() -> _auditLogFilter.filter(_requestContext, _responseContext))
        .doesNotThrowAnyException();

    // Then - main response should not be affected
    verify(_responseContext, never()).setStatus(anyInt());
    verify(_responseContext, never()).setEntity(any());
  }

  @Test
  public void testDurationCalculation() throws IOException, InterruptedException {
    // Given
    _config.setCaptureResponseEnabled(true);
    when(_responseContext.getStatus()).thenReturn(200);

    AuditEvent requestEvent = new AuditEvent();
    when(_auditRequestProcessor.processRequest(any(), anyString())).thenReturn(requestEvent);

    // When - request filter
    long startTime = System.nanoTime();
    _auditLogFilter.filter(_requestContext);

    // Capture context
    ArgumentCaptor<AuditResponseContext> contextCaptor = ArgumentCaptor.forClass(AuditResponseContext.class);
    verify(_requestContext).setProperty(eq("audit.response.context"), contextCaptor.capture());
    AuditResponseContext capturedContext = contextCaptor.getValue();

    // Simulate some processing time
    Thread.sleep(10);

    // Set up context retrieval
    when(_requestContext.getProperty("audit.response.context")).thenReturn(capturedContext);

    // When - response filter
    _auditLogFilter.filter(_requestContext, _responseContext);
    long endTime = System.nanoTime();

    // Then
    ArgumentCaptor<AuditEvent> eventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
    _auditLoggerMock.verify(() -> AuditLogger.auditLog(eventCaptor.capture()), times(2));

    AuditEvent responseEvent = eventCaptor.getAllValues().get(1);
    assertThat(responseEvent.getDurationMs()).isNotNull();
    assertThat(responseEvent.getDurationMs()).isGreaterThanOrEqualTo(10L);
    // Verify duration is within reasonable bounds
    long maxExpectedDuration = (endTime - startTime) / 1_000_000;
    assertThat(responseEvent.getDurationMs()).isLessThanOrEqualTo(maxExpectedDuration + 5);
  }

  @DataProvider(name = "auditConfigCombinations")
  public Object[][] provideAuditConfigCombinations() {
    return new Object[][]{
        // mainEnabled, responseEnabled, shouldAuditResponse, description
        {false, false, false, "Both flags disabled - no auditing"},
        {true, false, false, "Only main audit enabled - no response auditing"},
        {false, true, false, "Only response enabled - still no auditing (main flag required)"},
        {true, true, true, "Both flags enabled - response auditing occurs"}
    };
  }

  @Test(dataProvider = "auditConfigCombinations")
  public void testResponseFilterWithConfigCombinations(boolean mainEnabled, boolean responseEnabled,
      boolean shouldAuditResponse, String testScenario) throws IOException {
    // Test scenario provides context for what we're testing
    assertThat(testScenario).isNotNull(); // Document the scenario being tested

    // Reset mocks for clean state
    reset(_requestContext, _responseContext);
    _auditLoggerMock.clearInvocations();

    // Configure audit settings
    _config.setEnabled(mainEnabled);
    _config.setCaptureResponseEnabled(responseEnabled);

    // Set up response context as if request filter had run
    AuditResponseContext context = new AuditResponseContext()
        .setRequestId(UUID.randomUUID().toString())
        .setStartTimeNanos(System.nanoTime());
    when(_requestContext.getProperty("audit.response.context")).thenReturn(context);
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_responseContext.getStatus()).thenReturn(200);

    // When
    _auditLogFilter.filter(_requestContext, _responseContext);

    // Then - verify based on expected behavior (description helps with test output)
    if (shouldAuditResponse) {
      _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), times(1));
    } else {
      _auditLoggerMock.verify(() -> AuditLogger.auditLog(any()), never());
    }
  }
}
