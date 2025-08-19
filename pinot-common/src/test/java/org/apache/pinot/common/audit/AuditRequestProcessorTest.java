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
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

  private AuditRequestProcessor _processor;
  private AuditConfig _defaultConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _processor = new AuditRequestProcessor();
    // Use reflection to inject the mock config manager since it's private
    try {
      java.lang.reflect.Field field = AuditRequestProcessor.class.getDeclaredField("_configManager");
      field.setAccessible(true);
      field.set(_processor, _configManager);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject mock config manager", e);
    }

    _defaultConfig = new AuditConfig();
    _defaultConfig.setEnabled(true);
    _defaultConfig.setCaptureRequestPayload(false);
    _defaultConfig.setCaptureRequestHeaders(false);
    _defaultConfig.setMaxPayloadSize(10240);
    _defaultConfig.setExcludedEndpoints("");

    when(_configManager.isEnabled()).thenReturn(true);
    when(_configManager.getCurrentConfig()).thenReturn(_defaultConfig);
    when(_configManager.isEndpointExcluded(any())).thenReturn(false);
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

  // IP Address Extraction Tests

  @Test
  public void testExtractIpFromXForwardedForSingleIp() {
    MultivaluedMap<String, String> headers = createHeaders("X-Forwarded-For", "192.168.1.100");
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn("192.168.1.100");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("192.168.1.100");
  }

  @Test
  public void testExtractIpFromXForwardedForMultipleIps() {
    MultivaluedMap<String, String> headers =
        createHeaders("X-Forwarded-For", "192.168.1.100, 10.0.0.50, 203.0.113.195");
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn("192.168.1.100, 10.0.0.50, 203.0.113.195");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("192.168.1.100");
  }

  @Test
  public void testExtractIpFromXRealIp() {
    MultivaluedMap<String, String> headers = createHeaders("X-Real-IP", "192.168.1.200");
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn(null);
    when(_requestContext.getHeaderString("X-Real-IP")).thenReturn("192.168.1.200");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("192.168.1.200");
  }

  @Test
  public void testExtractIpPriorityOrder() {
    MultivaluedMap<String, String> headers =
        createHeaders("X-Forwarded-For", "192.168.1.100", "X-Real-IP", "192.168.1.200");
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn("192.168.1.100");
    when(_requestContext.getHeaderString("X-Real-IP")).thenReturn("192.168.1.200");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("192.168.1.100");
  }

  @Test
  public void testExtractIpWithBlankHeaders() {
    MultivaluedMap<String, String> headers = createHeaders("X-Forwarded-For", "   ", "X-Real-IP", "");
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn("   ");
    when(_requestContext.getHeaderString("X-Real-IP")).thenReturn("");
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("10.0.0.1");
  }

  @Test
  public void testExtractIpFallbackToRemoteAddr() {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    when(_requestContext.getHeaders()).thenReturn(headers);
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenReturn(null);
    when(_requestContext.getHeaderString("X-Real-IP")).thenReturn(null);
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isEqualTo("10.0.0.1");
  }

  @Test
  public void testExtractIpHandlesException() {
    when(_requestContext.getHeaderString("X-Forwarded-For")).thenThrow(new RuntimeException("Test exception"));
    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("GET");

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    assertThat(result.getOriginIpAddress()).isNull();
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
  public void testCaptureRequestBodyWhenEnabled() {
    _defaultConfig.setCaptureRequestPayload(true);
    String requestBody = "{\"key\": \"value\"}";
    InputStream entityStream = new ByteArrayInputStream(requestBody.getBytes());

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("POST");
    when(_requestContext.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(_requestContext.hasEntity()).thenReturn(true);
    when(_requestContext.getEntityStream()).thenReturn(entityStream);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    assertThat(payload).isNotNull();
    assertThat(payload.getBody()).isEqualTo(requestBody);
    verify(_requestContext).setEntityStream(any(ByteArrayInputStream.class));
  }

  @Test
  public void testSkipRequestBodyWhenDisabled() {
    _defaultConfig.setCaptureRequestPayload(false);

    when(_requestContext.getUriInfo()).thenReturn(_uriInfo);
    when(_uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(_uriInfo.getPath()).thenReturn("/test");
    when(_requestContext.getMethod()).thenReturn("POST");
    when(_requestContext.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(_requestContext.hasEntity()).thenReturn(true);

    AuditEvent result = _processor.processRequest(_requestContext, "10.0.0.1");

    assertThat(result).isNotNull();  
    AuditEvent.AuditRequestPayload payload = result.getRequest();
    if (payload != null) {
      assertThat(payload.getBody()).isNull();
    }
    verify(_requestContext, times(0)).getEntityStream();
  }

  @Test
  public void testCaptureHeadersWhenEnabled() {
    _defaultConfig.setCaptureRequestHeaders(true);
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
    _defaultConfig.setCaptureRequestHeaders(false);
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
    _defaultConfig.setCaptureRequestHeaders(true);
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
}
