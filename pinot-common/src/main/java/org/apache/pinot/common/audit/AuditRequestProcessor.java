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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  static final String TRUNCATION_MARKER = "...[truncated]";

  private final AuditConfigManager _configManager;
  private final AuditIdentityResolver _identityResolver;
  private final AuditUrlPathFilter _auditUrlPathFilter;

  @Inject
  public AuditRequestProcessor(AuditConfigManager configManager, AuditIdentityResolver identityResolver,
      AuditUrlPathFilter auditUrlPathFilter) {
    _configManager = configManager;
    _identityResolver = identityResolver;
    _auditUrlPathFilter = auditUrlPathFilter;
  }

  /**
   * Converts a MultivaluedMap into a Map of query parameters.
   * If a key in the MultivaluedMap has a single value, that value is added directly to the resulting map.
   * If a key has multiple values, the list of values is added instead.
   *
   * @param multimap the input MultivaluedMap containing keys and their associated values
   * @param allowedKeys optional set of allowed keys for case-insensitive filtering.
   *                    If null or empty, all keys are included
   * @return a Map where each key is mapped to either a single value or a list of values
   */
  private static Map<String, Object> toMap(MultivaluedMap<String, String> multimap, Set<String> allowedKeys) {
    Map<String, Object> resultMap = new HashMap<>();
    boolean filterKeys = allowedKeys != null && !allowedKeys.isEmpty();

    for (Map.Entry<String, List<String>> entry : multimap.entrySet()) {
      String key = entry.getKey();
      // Skip if filtering is enabled and key is not in allowed list (case-insensitive)
      if (filterKeys && !allowedKeys.contains(key.toLowerCase())) {
        continue;
      }

      List<String> values = entry.getValue();
      if (values.size() == 1) {
        resultMap.put(key, values.get(0));
      } else {
        resultMap.put(key, values);
      }
    }
    return resultMap;
  }

  /**
   * Converts a MultivaluedMap into a Map of query parameters without filtering.
   * Backward compatibility method.
   *
   * @param multimap the input MultivaluedMap containing keys and their associated values
   * @return a Map where each key is mapped to either a single value or a list of values
   */
  private static Map<String, Object> toMap(MultivaluedMap<String, String> multimap) {
    return toMap(multimap, null);
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
      if (_auditUrlPathFilter.isExcluded(endpoint, _configManager.getCurrentConfig().getUrlFilterExcludePatterns())) {
        return null;
      }

      // Log the audit event (service ID will be extracted from headers, not config)
      return new AuditEvent().setTimestamp(Instant.now().toString())
          .setEndpoint(endpoint)
          .setServiceId(extractServiceId(requestContext))
          .setMethod(requestContext.getMethod())
          .setOriginIpAddress(extractClientIpAddress(requestContext, remoteAddr))
          .setUserid(_identityResolver.resolveIdentity(requestContext))
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

      Set<String> allowedHeaders = parseAllowedHeaders(config.getCaptureRequestHeaders());
      if (!allowedHeaders.isEmpty()) {
        MultivaluedMap<String, String> allHeaders = requestContext.getHeaders();
        if (!allHeaders.isEmpty()) {
          Map<String, Object> filteredHeaders = toMap(allHeaders, allowedHeaders);
          if (!filteredHeaders.isEmpty()) {
            payload.setHeaders(filteredHeaders);
          }
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
   * Parses a comma-separated list of headers into a Set of lowercase header names
   * for case-insensitive comparison.
   *
   * @param headerList comma-separated list of header names
   * @return Set of lowercase header names, empty if headerList is blank
   */
  @VisibleForTesting
  static Set<String> parseAllowedHeaders(String headerList) {
    if (StringUtils.isBlank(headerList)) {
      return Collections.emptySet();
    }

    Set<String> headers = new HashSet<>();
    for (String header : headerList.split(",")) {
      String trimmed = header.trim();
      if (!trimmed.isEmpty()) {
        // Store as lowercase for case-insensitive comparison
        headers.add(trimmed.toLowerCase());
      }
    }
    return headers;
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
  @VisibleForTesting
  String readRequestBody(ContainerRequestContext requestContext, int maxPayloadSize) {
    if (!requestContext.hasEntity()) {
      return null;
    }

    final InputStream originalStream = requestContext.getEntityStream();
    if (originalStream == null) {
      return null;
    }

    try {
      final int bufferSize = Math.min(maxPayloadSize + 1024, AuditConfig.MAX_AUDIT_PAYLOAD_SIZE_BYTES);
      final BufferedInputStream bufferedStream = new BufferedInputStream(originalStream, bufferSize);
      requestContext.setEntityStream(bufferedStream);
      bufferedStream.mark(maxPayloadSize + 1);

      final InputStream limitedStream = ByteStreams.limit(bufferedStream, maxPayloadSize);
      final byte[] capturedBytes = ByteStreams.toByteArray(limitedStream);

      try {
        bufferedStream.reset();
      } catch (IOException resetException) {
        // error because it can affect downstream consumers from processing the request. This API call will fail.
        LOG.error("Failed to reset stream for downstream consumers", resetException);
      }

      if (capturedBytes.length > 0) {
        String requestBody = new String(capturedBytes, StandardCharsets.UTF_8);
        if (capturedBytes.length >= maxPayloadSize) {
          requestBody += TRUNCATION_MARKER;
        }
        return requestBody;
      }
    } catch (IOException e) {
      LOG.warn("Failed to capture request body", e);
    }
    return null;
  }
}
