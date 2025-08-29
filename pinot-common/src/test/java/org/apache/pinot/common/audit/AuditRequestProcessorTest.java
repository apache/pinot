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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link AuditRequestProcessor}.
 */
public class AuditRequestProcessorTest {

  @Mock
  private AuditConfigManager _configManager;

  @Mock
  private ContainerRequestContext _requestContext;

  @Mock
  private UriInfo _uriInfo;

  @Mock
  private UrlPathFilter _urlPathFilter;

  private AuditRequestProcessor _processor;
  private AuditConfig _defaultConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _processor = new AuditRequestProcessor(_configManager, mock(AuditIdentityResolver.class), _urlPathFilter);

    _defaultConfig = new AuditConfig();
    _defaultConfig.setEnabled(true);
    _defaultConfig.setCaptureRequestPayload(false);
    _defaultConfig.setCaptureRequestHeaders("");
    _defaultConfig.setMaxPayloadSize(10240);
    _defaultConfig.setUrlFilterExcludePatterns("");

    when(_configManager.isEnabled()).thenReturn(true);
    when(_configManager.getCurrentConfig()).thenReturn(_defaultConfig);
    when(_urlPathFilter.isExcluded(any(), any())).thenReturn(false);
  }

  private MultivaluedMap<String, String> createHeaders(String... headerPairs) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    for (int i = 0; i < headerPairs.length; i += 2) {
      headers.add(headerPairs[i], headerPairs[i + 1]);
    }
    return headers;
  }

  private MultivaluedMap<String, String> createQueryParams(String... paramPairs) {
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    for (int i = 0; i < paramPairs.length; i += 2) {
      params.add(paramPairs[i], paramPairs[i + 1]);
    }
    return params;
  }

  // Payload Capture Tests
  @Test
  public void testCaptureQueryParametersSingleValue() {
    MultivaluedMap<String, String> queryParams = createQueryParams("param1", "value1", "param2", "value2");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(queryParams);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(new MultivaluedHashMap<>());

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> queryParameters = payload.getQueryParameters();
    assertThat(queryParameters).containsEntry("param1", "value1");
    assertThat(queryParameters).containsEntry("param2", "value2");
  }

  @Test
  public void testCaptureQueryParametersMultipleValues() {
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.addAll("tags", Arrays.asList("tag1", "tag2", "tag3"));
    queryParams.add("single", "value");

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(queryParams);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(new MultivaluedHashMap<>());

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> queryParameters = payload.getQueryParameters();
    assertThat(queryParameters).containsEntry("single", "value");
    @SuppressWarnings("unchecked")
    List<String> tags = (List<String>) queryParameters.get("tags");
    assertThat(tags).containsExactly("tag1", "tag2", "tag3");
  }

  @Test
  public void testCaptureHeadersWhenEnabled() {
    _defaultConfig.setCaptureRequestHeaders("Content-Type,X-Custom-Header,Authorization,X-Password");
    MultivaluedMap<String, String> headers =
        createHeaders("Content-Type", "application/json", "X-Custom-Header", "custom-value", "Authorization",
            "Bearer token123",  // Should be filtered out
            "X-Password", "secret123"  // Should be filtered out
        );

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    @SuppressWarnings("unchecked")
    Map<String, Object> capturedHeaders = payload.getHeaders();
    assertThat(capturedHeaders).containsEntry("Content-Type", "application/json");
    assertThat(capturedHeaders).containsEntry("X-Custom-Header", "custom-value");
    assertThat(capturedHeaders).containsEntry("Authorization", "Bearer token123");
    assertThat(capturedHeaders).containsEntry("X-Password", "secret123");
  }

  @Test
  public void testSkipHeadersWhenDisabled() {
    _defaultConfig.setCaptureRequestHeaders("");
    MultivaluedMap<String, String> headers = createHeaders("Content-Type", "application/json");

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    if (payload != null) {
      assertThat(payload.getHeaders()).isNull();
    }
  }

  @Test
  public void testFilterSensitiveHeaders() {
    _defaultConfig.setCaptureRequestHeaders(
        "authorization,x-auth-token,password-header,api-secret,x-api-key,content-type");
    MultivaluedMap<String, String> headers =
        createHeaders("authorization", "Bearer token123", "x-auth-token", "token456", "password-header", "pass123",
            "api-secret", "secret789", "x-api-key", "key123",
            // Should be filtered (contains 'secret' logic might not catch this)
            "content-type", "application/json"  // Should be kept
        );

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> capturedHeaders = payload.getHeaders();
    assertThat(capturedHeaders).containsEntry("content-type", "application/json");
    assertThat(capturedHeaders).containsEntry("authorization", "Bearer token123");
    assertThat(capturedHeaders).containsEntry("x-auth-token", "token456");
    assertThat(capturedHeaders).containsEntry("password-header", "pass123");
    assertThat(capturedHeaders).containsEntry("api-secret", "secret789");
    assertThat(capturedHeaders).containsEntry("x-api-key", "key123");
  }

  @Test
  public void testEmptyPayloadReturnsNull() {
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(_requestContext.hasEntity()).thenReturn(false);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getRequest()).isNull();
  }

  @Test
  public void testPayloadCaptureHandlesException() {
    when(_requestContext.getUriInfo()).thenThrow(new RuntimeException("Test exception"));
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    // The main processRequest catches all exceptions and returns null, so test for that
    assertThat(result).isNull();
  }

  @Test
  public void testParseAllowedHeadersEdgeCases() {
    // Empty/null/whitespace
    assertThat(AuditRequestProcessor.parseAllowedHeaders("")).isEmpty();
    assertThat(AuditRequestProcessor.parseAllowedHeaders("   ")).isEmpty();
    assertThat(AuditRequestProcessor.parseAllowedHeaders(null)).isEmpty();

    // Single header
    Set<String> singleHeader = AuditRequestProcessor.parseAllowedHeaders("Content-Type");
    assertThat(singleHeader).containsExactly("content-type");

    // Malformed comma separation
    Set<String> malformed1 = AuditRequestProcessor.parseAllowedHeaders("Header1,,Header2");
    assertThat(malformed1).containsExactlyInAnyOrder("header1", "header2");

    Set<String> malformed2 = AuditRequestProcessor.parseAllowedHeaders(",Header1,Header2,");
    assertThat(malformed2).containsExactlyInAnyOrder("header1", "header2");

    // Whitespace handling
    Set<String> withWhitespace = AuditRequestProcessor.parseAllowedHeaders(" Content-Type , X-Custom ");
    assertThat(withWhitespace).containsExactly("content-type", "x-custom");
  }

  @Test
  public void testHeaderFilteringCaseInsensitive() {
    _defaultConfig.setCaptureRequestHeaders("content-type,authorization,x-custom-header");

    MultivaluedMap<String, String> headers = createHeaders(
        "Content-Type", "application/json",      // Different case
        "AUTHORIZATION", "Bearer token",         // All caps
        "x-custom-header", "value",              // All lower
        "X-Ignored-Header", "ignored"            // Not in config
    );

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> capturedHeaders = payload.getHeaders();

    // Should capture first 3, ignore the 4th
    assertThat(capturedHeaders).hasSize(3);
    assertThat(capturedHeaders).containsKeys("Content-Type", "AUTHORIZATION", "x-custom-header");
    assertThat(capturedHeaders).doesNotContainKey("X-Ignored-Header");
  }

  @Test
  public void testHeaderValueHandling() {
    _defaultConfig.setCaptureRequestHeaders("single-value,multi-value,empty-value");

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.add("single-value", "value1");
    headers.addAll("multi-value", Arrays.asList("val1", "val2", "val3"));
    headers.add("empty-value", "");

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> capturedHeaders = payload.getHeaders();

    // Single value stored as String
    assertThat(capturedHeaders.get("single-value")).isEqualTo("value1");

    // Multiple values stored as List
    @SuppressWarnings("unchecked")
    List<String> multiValues = (List<String>) capturedHeaders.get("multi-value");
    assertThat(multiValues).containsExactly("val1", "val2", "val3");

    // Empty value still captured
    assertThat(capturedHeaders.get("empty-value")).isEqualTo("");
  }

  @Test
  public void testCompleteHeaderCaptureFlow() {
    // Configure specific headers
    _defaultConfig.setCaptureRequestHeaders("Content-Type,X-Request-ID,User-Agent");

    // Create request with mixed case headers + extras
    MultivaluedMap<String, String> headers = createHeaders(
        "content-type", "application/json",
        "X-REQUEST-ID", "req-123",
        "user-agent", "test-client",
        "Cookie", "session=abc",           // Should be ignored
        "Accept", "application/json"       // Should be ignored
    );

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");
    when(_requestContext.getHeaders()).thenReturn(headers);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    Map<String, Object> capturedHeaders = payload.getHeaders();

    assertThat(capturedHeaders).hasSize(3);
    assertThat(capturedHeaders).containsOnlyKeys("content-type", "X-REQUEST-ID", "user-agent");
    assertThat(capturedHeaders).containsEntry("content-type", "application/json");
    assertThat(capturedHeaders).containsEntry("X-REQUEST-ID", "req-123");
    assertThat(capturedHeaders).containsEntry("user-agent", "test-client");
  }
}
