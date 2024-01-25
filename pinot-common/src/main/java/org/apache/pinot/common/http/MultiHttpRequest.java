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
package org.apache.pinot.common.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to support multiple http operations in parallel by using the executor that is passed in. This is a wrapper
 * around Apache common HTTP client.
 */
public class MultiHttpRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiHttpRequest.class);

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  /**
   * @param executor executor service to use for making parallel requests
   * @param connectionManager http connection manager to use.
   */
  public MultiHttpRequest(Executor executor, HttpClientConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * GET urls in parallel using the executor service.
   * @param urls absolute URLs to GET
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each GET request
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public CompletionService<MultiHttpRequestResponse> executeGet(List<String> urls,
      @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    List<Pair<String, String>> urlsAndRequestBodies = new ArrayList<>();
    urls.forEach(url -> urlsAndRequestBodies.add(Pair.of(url, "")));
    return execute(urlsAndRequestBodies, requestHeaders, timeoutMs, "GET", HttpGet::new);
  }

  /**
   * POST urls in parallel using the executor service.
   * @param urlsAndRequestBodies absolute URLs to POST
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each POST request
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public CompletionService<MultiHttpRequestResponse> executePost(List<Pair<String, String>> urlsAndRequestBodies,
      @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    return execute(urlsAndRequestBodies, requestHeaders, timeoutMs, "POST", HttpPost::new);
  }

  /**
   * Execute certain http method on the urls in parallel using the executor service.
   * @param urlsAndRequestBodies absolute URLs to execute the http method
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each http request
   * @param httpMethodName the name of the http method like GET, DELETE etc.
   * @param httpRequestBaseSupplier a function to create a new http method object.
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public <T extends HttpRequestBase> CompletionService<MultiHttpRequestResponse> execute(
      List<Pair<String, String>> urlsAndRequestBodies, @Nullable Map<String, String> requestHeaders, int timeoutMs,
      String httpMethodName, Function<String, T> httpRequestBaseSupplier) {
    // Create global request configuration
    RequestConfig defaultRequestConfig =
        RequestConfig.custom().setConnectionRequestTimeout(timeoutMs).setSocketTimeout(timeoutMs)
            .build(); // setting the socket

    HttpClientBuilder httpClientBuilder =
        HttpClients.custom().setConnectionManager(_connectionManager).setDefaultRequestConfig(defaultRequestConfig);

    CompletionService<MultiHttpRequestResponse> completionService = new ExecutorCompletionService<>(_executor);
    CloseableHttpClient client = httpClientBuilder.build();
    for (Pair<String, String> pair : urlsAndRequestBodies) {
      completionService.submit(() -> {
        String url = pair.getLeft();
        String body = pair.getRight();
        HttpRequestBase httpMethod = httpRequestBaseSupplier.apply(url);
        // If the http method is POST, set the request body
        if (httpMethod instanceof HttpPost) {
          ((HttpPost) httpMethod).setEntity(new StringEntity(body));
        }
        if (requestHeaders != null) {
          requestHeaders.forEach(((HttpRequestBase) httpMethod)::setHeader);
        }
        CloseableHttpResponse response = null;
        try {
          response = client.execute(httpMethod);
          return new MultiHttpRequestResponse(httpMethod.getURI(), response);
        } catch (IOException ex) {
          if (response != null) {
            String error = EntityUtils.toString(response.getEntity());
            LOGGER.warn("Caught '{}' while executing: {} on URL: {}", error, httpMethodName, url);
          } else {
            // Log only exception type and message instead of the whole stack trace
            LOGGER.warn("Caught '{}' while executing: {} on URL: {}", ex, httpMethodName, url);
          }
          throw ex;
        }
      });
    }
    return completionService;
  }
}
