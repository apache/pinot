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
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jersey filter for audit logging of API responses.
 * Captures response status code and correlates with request via request ID.
 */
@javax.ws.rs.ext.Provider
@Singleton
public class AuditResponseFilter implements ContainerResponseFilter {

  private static final Logger LOG = LoggerFactory.getLogger(AuditResponseFilter.class);

  private final AuditConfigManager _configManager;

  @Inject
  public AuditResponseFilter(AuditConfigManager configManager) {
    _configManager = configManager;
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    // Check if response auditing is enabled
    if (!_configManager.isEnabled() || !_configManager.getCurrentConfig().isCaptureResponseEnabled()) {
      return;
    }

    try {
      // Retrieve the request ID that was stored by AuditLogFilter
      String requestId = (String) requestContext.getProperty("audit.request.id");
      if (requestId == null) {
        // If no request ID found, skip response auditing
        return;
      }

      // Create an audit event for the response
      AuditEvent responseAuditEvent = new AuditEvent();
      responseAuditEvent.setRequestId(requestId);
      responseAuditEvent.setTimestamp(Instant.now().toString());
      responseAuditEvent.setResponseCode(responseContext.getStatus());

      // Include endpoint and method for context
      UriInfo uriInfo = requestContext.getUriInfo();
      responseAuditEvent.setEndpoint(uriInfo.getPath());
      responseAuditEvent.setMethod(requestContext.getMethod());

      // Log the response audit event
      AuditLogger.auditLog(responseAuditEvent);
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main response
      LOG.warn("Failed to process audit logging for response", e);
    }
  }
}
