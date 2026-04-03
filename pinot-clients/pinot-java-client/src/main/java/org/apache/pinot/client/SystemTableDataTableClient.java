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
package org.apache.pinot.client;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder;
import org.asynchttpclient.Dsl;


/**
 * Client for fetching system table {@link DataTable} payloads from broker endpoints.
 */
public final class SystemTableDataTableClient implements AutoCloseable {
  private static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
  private static final SslContextProvider SSL_CONTEXT_PROVIDER = SslContextProviderFactory.create();

  private final AsyncHttpClient _httpClient;
  private final int _requestTimeoutMs;
  private final Map<String, String> _defaultHeaders;

  public SystemTableDataTableClient(ConnectionTimeouts connectionTimeouts, @Nullable SSLContext sslContext) {
    this(Map.of(), connectionTimeouts, sslContext, TlsProtocols.defaultProtocols(false), null);
  }

  public SystemTableDataTableClient(Map<String, String> defaultHeaders, ConnectionTimeouts connectionTimeouts,
      @Nullable SSLContext sslContext, TlsProtocols tlsProtocols, @Nullable String appId) {
    _requestTimeoutMs = connectionTimeouts.getReadTimeoutMs();
    _defaultHeaders = defaultHeaders != null ? new HashMap<>(defaultHeaders) : new HashMap<>();
    Builder builder = Dsl.config();
    SSL_CONTEXT_PROVIDER.configure(builder, sslContext, tlsProtocols);
    builder.setRequestTimeout(Duration.ofMillis(_requestTimeoutMs))
        .setReadTimeout(Duration.ofMillis(connectionTimeouts.getReadTimeoutMs()))
        .setConnectTimeout(Duration.ofMillis(connectionTimeouts.getConnectTimeoutMs()))
        .setHandshakeTimeout(connectionTimeouts.getHandshakeTimeoutMs())
        .setUserAgent(ConnectionUtils.getUserAgentVersionFromClassPath("ua", appId));
    _httpClient = Dsl.asyncHttpClient(builder.build());
  }

  public DataTable fetchDataTable(String url, String requestBody, @Nullable Map<String, String> headers)
      throws PinotClientException {
    try {
      return fetchDataTableAsync(url, requestBody, headers).get(_requestTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  public CompletableFuture<DataTable> fetchDataTableAsync(String url, String requestBody,
      @Nullable Map<String, String> headers) {
    try {
      BoundRequestBuilder requestBuilder = _httpClient.preparePost(url);
      Map<String, String> mergedHeaders = new HashMap<>(_defaultHeaders);
      if (headers != null && !headers.isEmpty()) {
        mergedHeaders.putAll(headers);
      }
      mergedHeaders.putIfAbsent("Content-Type", CONTENT_TYPE_JSON);
      mergedHeaders.forEach(requestBuilder::addHeader);
      return requestBuilder.setBody(requestBody).execute().toCompletableFuture().thenApply(response -> {
        int status = response.getStatusCode();
        if (status != 200) {
          throw new PinotClientException("HTTP status " + status + ": " + response.getResponseBody());
        }
        byte[] payload = response.getResponseBodyAsBytes();
        if (payload == null || payload.length == 0) {
          throw new PinotClientException("Empty response body");
        }
        try {
          return DataTableFactory.getDataTable(payload);
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      });
    } catch (Exception e) {
      return CompletableFuture.failedFuture(new PinotClientException(e));
    }
  }

  @Override
  public void close()
      throws PinotClientException {
    if (_httpClient.isClosed()) {
      return;
    }
    try {
      _httpClient.close();
    } catch (Exception e) {
      throw new PinotClientException("Error while closing connection!", e);
    }
  }
}
