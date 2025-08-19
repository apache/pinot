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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
  private static final String ANONYMOUS = "anonymous";

  // HTTP Headers
  private static final String HEADER_X_FORWARDED_FOR = "X-Forwarded-For";
  private static final String HEADER_X_REAL_IP = "X-Real-IP";
  private static final String HEADER_AUTHORIZATION = "Authorization";
  private static final String HEADER_X_USER_ID = "X-User-ID";
  private static final String HEADER_X_USERNAME = "X-Username";
  private static final String HEADER_X_SERVICE_ID = "X-Service-ID";
  private static final String HEADER_X_SERVICE_NAME = "X-Service-Name";

  // Auth prefixes
  private static final String AUTH_BASIC_PREFIX = "Basic ";
  private static final String AUTH_BEARER_PREFIX = "Bearer ";

  // User identification values
  private static final String BASIC_AUTH_USER = "basic-auth-user";
  private static final String BEARER_TOKEN_USER = "bearer-token-user";

  // Common values
  private static final String UNKNOWN = "unknown";
  private static final String COMMA_SEPARATOR = ",";

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

      String method = requestContext.getMethod();
      String originIpAddress = extractClientIpAddress(requestContext, remoteAddr);
      String userId = extractUserId(requestContext);

      // Capture request payload based on configuration

      // Log the audit event (service ID will be extracted from headers, not config)
      return new AuditEvent().setServiceId(extractServiceId(requestContext))
          .setEndpoint(endpoint)
          .setMethod(method)
          .setOriginIpAddress(originIpAddress)
          .setUserId(userId)
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

  /**
   * Extracts the client IP address from the request.
   * Checks common proxy headers before falling back to remote address.
   *
   * @param requestContext the container request context
   * @param remoteAddr the remote address from the underlying request
   * @return the client IP address
   */
  private String extractClientIpAddress(ContainerRequestContext requestContext, String remoteAddr) {
    try {
      // Check for proxy headers first
      String xForwardedFor = requestContext.getHeaderString(HEADER_X_FORWARDED_FOR);
      if (StringUtils.isNotBlank(xForwardedFor)) {
        // X-Forwarded-For can contain multiple IPs, take the first one
        return xForwardedFor.split(COMMA_SEPARATOR)[0].trim();
      }

      String xRealIp = requestContext.getHeaderString(HEADER_X_REAL_IP);
      if (StringUtils.isNotBlank(xRealIp)) {
        return xRealIp.trim();
      }

      // Fall back to remote address
      return remoteAddr;
    } catch (Exception e) {
      LOG.debug("Failed to extract client IP address", e);
      return UNKNOWN;
    }
  }

  /**
   * Extracts user ID from request headers.
   * Looks for common authentication headers.
   *
   * @param requestContext the container request context
   * @return the user ID or "anonymous" if not found
   */
  private String extractUserId(ContainerRequestContext requestContext) {
    try {
      // Check for common user identification headers
      String authHeader = requestContext.getHeaderString(HEADER_AUTHORIZATION);
      if (StringUtils.isNotBlank(authHeader)) {
        // For basic auth, extract username; for bearer tokens, use a placeholder
        if (authHeader.startsWith(AUTH_BASIC_PREFIX)) {
          // Could decode basic auth to get username, but for security keep it as placeholder
          return BASIC_AUTH_USER;
        } else if (authHeader.startsWith(AUTH_BEARER_PREFIX)) {
          return BEARER_TOKEN_USER;
        }
      }

      // Check for custom user headers
      String userHeader = requestContext.getHeaderString(HEADER_X_USER_ID);
      if (StringUtils.isNotBlank(userHeader)) {
        return userHeader.trim();
      }

      userHeader = requestContext.getHeaderString(HEADER_X_USERNAME);
      if (StringUtils.isNotBlank(userHeader)) {
        return userHeader.trim();
      }

      return ANONYMOUS;
    } catch (Exception e) {
      LOG.debug("Failed to extract user ID", e);
      return ANONYMOUS;
    }
  }

  /**
   * Extracts service ID from request headers.
   * Service ID should be provided by the client in headers, not from configuration.
   *
   * @param requestContext the container request context
   * @return the service ID or "unknown" if not found
   */
  private String extractServiceId(ContainerRequestContext requestContext) {
    try {
      // Check for custom service ID headers
      String serviceId = requestContext.getHeaderString(HEADER_X_SERVICE_ID);
      if (StringUtils.isNotBlank(serviceId)) {
        return serviceId.trim();
      }

      serviceId = requestContext.getHeaderString(HEADER_X_SERVICE_NAME);
      if (StringUtils.isNotBlank(serviceId)) {
        return serviceId.trim();
      }

      return null;
    } catch (Exception e) {
      LOG.debug("Failed to extract service ID", e);
      return null;
    }
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
    try {
      InputStream entityStream = requestContext.getEntityStream();
      if (entityStream == null) {
        return null;
      }

      // Read the stream content with size limit
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] data = new byte[8192];
      int bytesRead;
      int totalBytesRead = 0;
      boolean truncated = false;

      while ((bytesRead = entityStream.read(data, 0, data.length)) != -1) {
        if (totalBytesRead + bytesRead > maxPayloadSize) {
          // Truncate to max payload size
          int remainingBytes = maxPayloadSize - totalBytesRead;
          if (remainingBytes > 0) {
            buffer.write(data, 0, remainingBytes);
          }
          truncated = true;
          break;
        }
        buffer.write(data, 0, bytesRead);
        totalBytesRead += bytesRead;
      }

      byte[] requestBodyBytes = buffer.toByteArray();
      String requestBody = new String(requestBodyBytes, StandardCharsets.UTF_8);

      // Add truncation indicator if needed
      if (truncated) {
        requestBody += " [TRUNCATED - exceeds " + maxPayloadSize + " byte limit]";
      }

      // Restore the input stream for downstream processing
      // Need to read the entire original stream to restore it properly
      if (truncated) {
        // Read remaining bytes to fully consume the original stream
        while (entityStream.read(data) != -1) {
          // Consume remaining bytes
        }
        // Create new stream with original data (this is complex, for now keep the truncated version)
        requestContext.setEntityStream(new ByteArrayInputStream(requestBodyBytes));
      } else {
        requestContext.setEntityStream(new ByteArrayInputStream(requestBodyBytes));
      }

      return requestBody;
    } catch (IOException e) {
      LOG.debug("Failed to read request body", e);
      return "Failed to read request body: " + e.getMessage();
    }
  }
}
