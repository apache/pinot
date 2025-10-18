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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ClientStats;
import org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HTTP transport for Pinot admin operations.
 * Handles communication with Pinot controller REST APIs.
 */
public class PinotAdminTransport implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdminTransport.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Gets the ObjectMapper instance for JSON serialization/deserialization.
   *
   * @return ObjectMapper instance
   */
  public static ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }

  private final AsyncHttpClient _httpClient;
  private final String _scheme;
  private final Map<String, String> _defaultHeaders;
  private final int _requestTimeoutMs;

  public PinotAdminTransport(Properties properties, Map<String, String> authHeaders) {
    _defaultHeaders = authHeaders != null ? authHeaders : Map.of();

    // Extract timeout configuration
    _requestTimeoutMs = Integer.parseInt(properties.getProperty("pinot.admin.request.timeout.ms", "60000"));

    // Extract scheme (http/https)
    String scheme = properties.getProperty("pinot.admin.scheme", CommonConstants.HTTP_PROTOCOL);
    _scheme = scheme;

    // Build HTTP client
    Builder builder = Dsl.config()
        .setRequestTimeout(Duration.ofMillis(_requestTimeoutMs))
        .setReadTimeout(Duration.ofMillis(_requestTimeoutMs))
        .setConnectTimeout(Duration.ofMillis(10000)) // 10 second connect timeout
        .setUserAgent(ConnectionUtils.getUserAgentVersionFromClassPath("ua", null));

    // Configure SSL if needed
    if (CommonConstants.HTTPS_PROTOCOL.equalsIgnoreCase(scheme)) {
      try {
        SSLContext sslContext = SSLContext.getDefault();
        builder.setSslContext(new io.netty.handler.ssl.JdkSslContext(sslContext, true,
            io.netty.handler.ssl.ClientAuth.OPTIONAL));
      } catch (Exception e) {
        LOGGER.warn("Failed to configure SSL context, proceeding without SSL", e);
      }
    }

    _httpClient = Dsl.asyncHttpClient(builder.build());
    LOGGER.info("Initialized Pinot admin transport with scheme: {}, timeout: {}ms", scheme, _requestTimeoutMs);
  }

  /**
   * Executes a GET request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return Response JSON node
   * @throws PinotAdminException If the request fails
   */
  public JsonNode executeGet(String controllerAddress, String path, Map<String, String> queryParams,
      Map<String, String> headers)
      throws PinotAdminException {
    try {
      return executeGetAsync(controllerAddress, path, queryParams, headers).get(_requestTimeoutMs,
          java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new PinotAdminException("Failed to execute GET request to " + path, e);
    }
  }

  /**
   * Executes an async GET request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return CompletableFuture with response JSON node
   */
  public CompletableFuture<JsonNode> executeGetAsync(String controllerAddress, String path,
      Map<String, String> queryParams, Map<String, String> headers) {
    return executeRequestAsync(controllerAddress, path, "GET", null, queryParams, headers)
        .thenApply(this::parseResponse)
        .exceptionally(throwable -> {
          LOGGER.error("Failed to execute GET request to " + path, throwable);
          throw new RuntimeException("Failed to execute GET request", throwable);
        });
  }

  /**
   * Executes a POST request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param body Request body
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return Response JSON node
   * @throws PinotAdminException If the request fails
   */
  public JsonNode executePost(String controllerAddress, String path, Object body,
      Map<String, String> queryParams, Map<String, String> headers)
      throws PinotAdminException {
    try {
      return executePostAsync(controllerAddress, path, body, queryParams, headers).get(_requestTimeoutMs,
          java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new PinotAdminException("Failed to execute POST request to " + path, e);
    }
  }

  /**
   * Executes an async POST request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param body Request body
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return CompletableFuture with response JSON node
   */
  public CompletableFuture<JsonNode> executePostAsync(String controllerAddress, String path, Object body,
      Map<String, String> queryParams, Map<String, String> headers) {
    return executeRequestAsync(controllerAddress, path, "POST", body, queryParams, headers)
        .thenApply(this::parseResponse)
        .exceptionally(throwable -> {
          LOGGER.error("Failed to execute POST request to " + path, throwable);
          throw new RuntimeException("Failed to execute POST request", throwable);
        });
  }

  /**
   * Executes a PUT request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param body Request body
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return Response JSON node
   * @throws PinotAdminException If the request fails
   */
  public JsonNode executePut(String controllerAddress, String path, Object body,
      Map<String, String> queryParams, Map<String, String> headers)
      throws PinotAdminException {
    try {
      return executePutAsync(controllerAddress, path, body, queryParams, headers).get(_requestTimeoutMs,
          java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new PinotAdminException("Failed to execute PUT request to " + path, e);
    }
  }

  /**
   * Executes an async PUT request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param body Request body
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return CompletableFuture with response JSON node
   */
  public CompletableFuture<JsonNode> executePutAsync(String controllerAddress, String path, Object body,
      Map<String, String> queryParams, Map<String, String> headers) {
    return executeRequestAsync(controllerAddress, path, "PUT", body, queryParams, headers)
        .thenApply(this::parseResponse)
        .exceptionally(throwable -> {
          LOGGER.error("Failed to execute PUT request to " + path, throwable);
          throw new RuntimeException("Failed to execute PUT request", throwable);
        });
  }

  /**
   * Executes a DELETE request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return Response JSON node
   * @throws PinotAdminException If the request fails
   */
  public JsonNode executeDelete(String controllerAddress, String path, Map<String, String> queryParams,
      Map<String, String> headers)
      throws PinotAdminException {
    try {
      return executeDeleteAsync(controllerAddress, path, queryParams, headers).get(_requestTimeoutMs,
          java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new PinotAdminException("Failed to execute DELETE request to " + path, e);
    }
  }

  /**
   * Executes an async DELETE request to the specified path.
   *
   * @param controllerAddress Controller address
   * @param path Request path
   * @param queryParams Query parameters
   * @param headers Additional headers
   * @return CompletableFuture with response JSON node
   */
  public CompletableFuture<JsonNode> executeDeleteAsync(String controllerAddress, String path,
      Map<String, String> queryParams, Map<String, String> headers) {
    return executeRequestAsync(controllerAddress, path, "DELETE", null, queryParams, headers)
        .thenApply(this::parseResponse)
        .exceptionally(throwable -> {
          LOGGER.error("Failed to execute DELETE request to " + path, throwable);
          throw new RuntimeException("Failed to execute DELETE request", throwable);
        });
  }

  private CompletableFuture<Response> executeRequestAsync(String controllerAddress, String path, String method,
      Object body, Map<String, String> queryParams, Map<String, String> headers) {
    String url = buildUrl(controllerAddress, path, queryParams);

    BoundRequestBuilder requestBuilder = _httpClient.prepare(method, url);

    // Add default headers
    for (Map.Entry<String, String> header : _defaultHeaders.entrySet()) {
      requestBuilder.addHeader(header.getKey(), header.getValue());
    }

    // Add request-specific headers
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        requestBuilder.addHeader(header.getKey(), header.getValue());
      }
    }

    // Set content type for requests with body
    if (body != null) {
      String bodyStr = body instanceof String ? (String) body : toJson(body);
      requestBuilder.setBody(bodyStr).addHeader("Content-Type", "application/json");
    }

    return requestBuilder.execute().toCompletableFuture();
  }

  String buildUrl(String controllerAddress, String path, Map<String, String> queryParams) {
    StringBuilder url = new StringBuilder(_scheme).append("://").append(controllerAddress).append(path);

    if (queryParams != null && !queryParams.isEmpty()) {
      url.append("?");
      boolean first = true;
      for (Map.Entry<String, String> param : queryParams.entrySet()) {
        if (!first) {
          url.append("&");
        }
        url.append(URLEncoder.encode(param.getKey(), StandardCharsets.UTF_8)).append("=");
        if (param.getValue() != null) {
          url.append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8));
        }
        first = false;
      }
    }

    return url.toString();
  }

  private String toJson(Object obj) {
    try {
      return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize object to JSON", e);
    }
  }

  /**
   * Parses a JSON array field into a List of Strings.
   * Handles both actual JSON arrays and comma-separated strings for backward compatibility.
   *
   * @param response JSON response node
   * @param fieldName Name of the field containing the array
   * @return List of strings from the array field
   * @throws PinotAdminException If the field is missing, null, or not in expected format
   */
  public List<String> parseStringArray(JsonNode response, String fieldName)
      throws PinotAdminException {
    JsonNode arrayNode = response.get(fieldName);
    if (arrayNode == null) {
      throw new PinotAdminException("Response missing '" + fieldName + "' field");
    }

    if (arrayNode.isArray()) {
      // Handle JSON array format
      java.util.List<String> result = new java.util.ArrayList<>();
      for (JsonNode element : arrayNode) {
        result.add(element.asText());
      }
      return result;
    } else if (arrayNode.isTextual()) {
      // Handle comma-separated string format for backward compatibility
      String text = arrayNode.asText().trim();
      if (text.isEmpty()) {
        return java.util.Collections.emptyList();
      }
      return java.util.Arrays.asList(text.split(","));
    } else {
      throw new PinotAdminException("Field '" + fieldName + "' is not an array or string: " + arrayNode.getNodeType());
    }
  }

  /**
   * Safely parses a JSON array field into a List of Strings for async operations.
   * Returns empty list on error instead of throwing exception.
   *
   * @param response JSON response node
   * @param fieldName Name of the field containing the array
   * @return List of strings from the array field, or empty list if parsing fails
   */
  public List<String> parseStringArraySafe(JsonNode response, String fieldName) {
    try {
      return parseStringArray(response, fieldName);
    } catch (PinotAdminException e) {
      LOGGER.warn("Failed to parse string array for field '{}': {}", fieldName, e.getMessage());
      return java.util.Collections.emptyList();
    }
  }

  private JsonNode parseResponse(Response response) {
    try {
      int statusCode = response.getStatusCode();
      String responseBody = response.getResponseBody();

      if (statusCode >= 200 && statusCode < 300) {
        if (responseBody == null || responseBody.trim().isEmpty()) {
          return OBJECT_MAPPER.createObjectNode();
        }
        return OBJECT_MAPPER.readTree(responseBody);
      } else {
        // Handle specific error cases
        if (statusCode == 401) {
          throw new PinotAdminAuthenticationException("Authentication failed: " + responseBody);
        } else if (statusCode == 403) {
          throw new PinotAdminAuthenticationException("Access forbidden: " + responseBody);
        } else if (statusCode == 404) {
          throw new PinotAdminNotFoundException("Resource not found: " + responseBody);
        } else if (statusCode >= 400 && statusCode < 500) {
          throw new PinotAdminValidationException("Client error (status: " + statusCode + "): " + responseBody);
        } else {
          throw new PinotAdminException("HTTP request failed with status: " + statusCode
              + ", body: " + responseBody);
        }
      }
    } catch (Exception e) {
      int statusCode = -1;
      String responseBodyExcerpt = "";
      try {
        if (response != null) {
          statusCode = response.getStatusCode();
          String body = response.getResponseBody();
          if (body != null) {
            responseBodyExcerpt = body.length() > 200 ? body.substring(0, 200) + "..." : body;
          }
        }
      } catch (Exception inner) {
        // Ignore, use defaults
      }
      LOGGER.warn("Failed to parse response (status: {}, body excerpt: '{}')", statusCode, responseBodyExcerpt, e);
      throw new RuntimeException(
          "Failed to parse response (status: " + statusCode + ", body excerpt: '" + responseBodyExcerpt
              + "', exception: " + e.getClass().getSimpleName() + ")",
          e);
    }
  }

  @Override
  public void close()
      throws IOException {
    if (_httpClient != null) {
      try {
        _httpClient.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close HTTP client", e);
      }
    }
  }

  /**
   * Gets the HTTP client statistics.
   *
   * @return Client statistics
   */
  public ClientStats getClientMetrics() {
    return _httpClient.getClientStats();
  }
}
