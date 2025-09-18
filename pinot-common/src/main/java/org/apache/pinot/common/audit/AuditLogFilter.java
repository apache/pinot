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
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.glassfish.grizzly.http.server.Request;


/**
 * Jersey filter for audit logging of API requests.
 * Supports dynamic configuration through injected AuditConfigManager.
 */
@javax.ws.rs.ext.Provider
@Singleton
public class AuditLogFilter implements ContainerRequestFilter {

  private final Provider<Request> _requestProvider;
  private final AuditRequestProcessor _auditRequestProcessor;

  @Inject
  public AuditLogFilter(Provider<Request> requestProvider, AuditRequestProcessor auditRequestProcessor) {
    _requestProvider = requestProvider;
    _auditRequestProcessor = auditRequestProcessor;
  }

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // Skip audit logging if it's not enabled to avoid unnecessary processing
    if (!_auditRequestProcessor.isEnabled()) {
      return;
    }

    // Generate a unique request ID for correlation
    String requestId = UUID.randomUUID().toString();
    requestContext.setProperty("audit.request.id", requestId);

    // Extract the remote address and delegate to the auditor
    final Request grizzlyRequest = _requestProvider.get();
    final String remoteAddr = grizzlyRequest.getRemoteAddr();

    final AuditEvent auditEvent = _auditRequestProcessor.processRequest(requestContext, remoteAddr);
    if (auditEvent != null) {
      // Add the request ID to the audit event
      auditEvent.setRequestId(requestId);
      AuditLogger.auditLog(auditEvent);
    }
  }
}
