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
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jersey filter for audit logging of API requests and responses.
 * Implements both request and response filters to capture full request-response cycle.
 * Supports dynamic configuration through injected AuditConfigManager.
 */
@javax.ws.rs.ext.Provider
@Singleton
public class AuditLogFilter implements ContainerRequestFilter, ContainerResponseFilter {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogFilter.class);
  private static final String PROPERTY_KEY_AUDIT_RESPONSE_CONTEXT = "audit.response.context";

  private final Provider<Request> _requestProvider;
  private final AuditRequestProcessor _auditRequestProcessor;
  private final AuditConfigManager _configManager;
  private final AuditMetrics _auditMetrics;

  @Inject
  public AuditLogFilter(Provider<Request> requestProvider, AuditRequestProcessor auditRequestProcessor,
      AuditConfigManager configManager, AuditMetrics auditMetrics) {
    _requestProvider = requestProvider;
    _auditRequestProcessor = auditRequestProcessor;
    _configManager = configManager;
    _auditMetrics = auditMetrics;
  }

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // Skip audit logging if it's not enabled to avoid unnecessary processing
    AuditConfig config = getCurrentConfig();
    if (!config.isEnabled()) {
      return;
    }

    measure(() -> filterRequest(requestContext, config), AuditMetrics.AuditTimer.AUDIT_REQUEST_PROCESSING_TIME,
        AuditMetrics.AuditMeter.AUDIT_REQUEST_FAILURES);
  }

  private void filterRequest(ContainerRequestContext requestContext, AuditConfig config) {
    AuditResponseContext responseContext = null;
    // Only create and store the context if response auditing is enabled
    if (config.isCaptureResponseEnabled()) {
      responseContext = new AuditResponseContext()
          .setRequestId(UUID.randomUUID().toString())
          .setStartTimeNanos(System.nanoTime());
      requestContext.setProperty(PROPERTY_KEY_AUDIT_RESPONSE_CONTEXT, responseContext);
    }

    // Extract the remote address and delegate to the auditor
    final Request grizzlyRequest = _requestProvider.get();
    final String remoteAddr = grizzlyRequest.getRemoteAddr();

    final AuditEvent auditEvent = _auditRequestProcessor.processRequest(requestContext, remoteAddr);
    if (auditEvent != null) {
      if (responseContext != null) {
        auditEvent.setRequestId(responseContext.getRequestId());
      }
      AuditLogger.auditLog(auditEvent);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    // Check if response auditing is enabled
    if (!getCurrentConfig().isEnabled() || !getCurrentConfig().isCaptureResponseEnabled()) {
      return;
    }

    measure(() -> filterResponse(requestContext, responseContext),
        AuditMetrics.AuditTimer.AUDIT_RESPONSE_PROCESSING_TIME, AuditMetrics.AuditMeter.AUDIT_RESPONSE_FAILURES);
  }

  private void filterResponse(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    // Retrieve the audit response context that was stored during request processing
    AuditResponseContext auditContext =
        (AuditResponseContext) requestContext.getProperty(PROPERTY_KEY_AUDIT_RESPONSE_CONTEXT);
    if (auditContext == null) {
      // If no context found, skip response auditing
      return;
    }

    // Extract the request ID from the context
    String requestId = auditContext.getRequestId();
    if (requestId == null) {
      return;
    }
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - auditContext.getStartTimeNanos());
    final AuditEvent auditEvent = new AuditEvent().setRequestId(requestId)
        .setTimestamp(Instant.now().toString())
        .setResponseCode(responseContext.getStatus())
        .setDurationMs(durationMs)
        .setEndpoint(requestContext.getUriInfo().getPath())
        .setMethod(requestContext.getMethod());

    AuditLogger.auditLog(auditEvent);
  }

  private void measure(Runnable operation, AuditMetrics.AuditTimer timer, AuditMetrics.AuditMeter failureMeter) {
    long startTime = System.nanoTime();
    try {
      operation.run();
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main response
      // logging the failure meter provides additional context if request/response
      LOG.warn("Failed to process audit logging. Incrementing {}", failureMeter, e);
      _auditMetrics.addMeteredGlobalValue(failureMeter, 1L);
    } finally {
      long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      _auditMetrics.addTimedValue(timer, durationMs);
    }
  }

  private AuditConfig getCurrentConfig() {
    return _configManager.getCurrentConfig();
  }
}
