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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for extracting audit information from Jersey HTTP requests.
 * Handles all the complex logic for IP address extraction, user identification,
 * and request payload capture for audit logging purposes.
 * Uses dynamic configuration to control audit behavior.
 */
@Singleton
public class AuditRequestProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(AuditRequestProcessor.class);

  @Inject
  private AuditConfigManager _configManager;

  /**
   * Converts a MultivaluedMap into a Map of query parameters.
   * If a key in the MultivaluedMap has a single value, that value is added directly to the resulting map.
   * If a key has multiple values, the list of values is added instead.
   *
   * @param multimap the input MultivaluedMap containing keys and their associated values
   * @return a Map where each key is mapped to either a single value or a list of values
   */
  private static Map<String, Object> toMap(MultivaluedMap<String, String> multimap) {
    Map<String, Object> queryMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : multimap.entrySet()) {
      List<String> values = entry.getValue();
      if (values.size() == 1) {
        queryMap.put(entry.getKey(), values.get(0));
      } else {
        queryMap.put(entry.getKey(), values);
      }
    }
    return queryMap;
  }

  public AuditEvent processRequest(ContainerRequestContext requestContext, String remoteAddr) {
    // Check if auditing is enabled (if config manager is available)
    if (!isEnabled()) {
      return null;
    }

    try {
      UriInfo uriInfo = requestContext.getUriInfo();
      String endpoint = uriInfo.getPath();

      // Check endpoint exclusions
      if (_configManager.isEndpointExcluded(endpoint)) {
        return null;
      }

      // Log the audit event (service ID will be extracted from headers, not config)
      return new AuditEvent().setTimestamp(Instant.now().toString())
          .setEndpoint(endpoint)
          .setServiceId(extractServiceId(requestContext))
          .setMethod(requestContext.getMethod())
          .setOriginIpAddress(extractClientIpAddress(requestContext, remoteAddr))
          .setUserId(extractUserId(requestContext))
          .setRequest(captureRequestPayload(requestContext));
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main request
      LOG.warn("Failed to process audit logging for request", e);
    }
    return null;
  }

  public boolean isEnabled() {
    return _configManager.isEnabled();
  }

  private String extractClientIpAddress(ContainerRequestContext requestContext, String remoteAddr) {
    // TODO spyne to be implemented
    return null;
  }

  /**
   * Extracts user ID from request headers.
   * Looks for common authentication headers.
   *
   * @param requestContext the container request context
   * @return the user ID or "anonymous" if not found
   */
  private String extractUserId(ContainerRequestContext requestContext) {
    // TODO spyne to be implemented
    return null;
  }

  /**
   * Extracts service ID from request headers.
   * Service ID should be provided by the client in headers, not from configuration.
   *
   * @param requestContext the container request context
   * @return the service ID or "unknown" if not found
   */
  private String extractServiceId(ContainerRequestContext requestContext) {
    // TODO spyne to be implemented
    return null;
  }

  private AuditEvent.AuditRequestPayload captureRequestPayload(ContainerRequestContext requestContext) {
    try {
      AuditEvent.AuditRequestPayload payload = new AuditEvent.AuditRequestPayload();
      UriInfo uriInfo = requestContext.getUriInfo();
      MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
      if (!queryParams.isEmpty()) {
        payload.setQueryParameters(toMap(queryParams));
      }

      final AuditConfig config = _configManager.getCurrentConfig();
      if (config.isCaptureRequestHeaders()) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        if (!headers.isEmpty()) {
          payload.setHeaders(toMap(headers));
        }
      }

      if (config.isCaptureRequestPayload() && requestContext.hasEntity()) {
        String requestBody = readRequestBody(requestContext, config.getMaxPayloadSize());
        if (StringUtils.isNotBlank(requestBody)) {
          payload.setBody(requestBody);
        }
      }

      if (payload.getQueryParameters() == null && payload.getBody() == null && payload.getHeaders() == null) {
        return null;
      }

      return payload;
    } catch (Exception e) {
      LOG.error("Failed to capture request payload", e);
      return new AuditEvent.AuditRequestPayload().setError("Failed to capture payload: " + e.getMessage());
    }
  }

  /**
   * Reads the request body from the entity input stream.
   * Restores the input stream for downstream processing.
   * Limits the amount of data read based on configuration.
   *
   * @param requestContext the request context
   * @param maxPayloadSize maximum bytes to read from the request body
   * @return the request body as string (potentially truncated)
   */
  private String readRequestBody(ContainerRequestContext requestContext, int maxPayloadSize) {
    // TODO spyne to be implemented
    return null;
  }
}
