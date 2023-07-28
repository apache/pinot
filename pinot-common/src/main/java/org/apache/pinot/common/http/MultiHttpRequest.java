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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to support multiple http operations in parallel by using the executor that is passed in. This is a wrapper
 * around Apache common HTTP client.
 *
 * The execute method is re-usable but there is no real benefit to it. All the connection management is handled by
 * the input HttpConnectionManager. Note that we cannot use SimpleHttpConnectionManager as it is not thread safe. Use
 * MultiThreadedHttpConnectionManager as shown in the example below. As GET is commonly used, there is a dedicated
 * execute method for it. Other http methods like DELETE can use the generic version of execute method.
 *
 * Usage:
 * <pre>
 * {@code
 *    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com");
 *    MultiHttpRequest mhr = new MultiHttpRequest(Executors.newCachedThreadPool(),
 *           new MultiThreadedHttpConnectionManager());
 *    CompletionService<GetMethod> completionService = mhr.execute(urls, headers, timeoutMs);
 *    for (int i = 0; i < urls.size(); i++) {
 *      GetMethod getMethod = null;
 *      try {
 *        getMethod = completionService.take().get();
 *        if (getMethod.getStatusCode() >= 300) {
 *          System.out.println("error");
 *          continue;
 *        }
 *        System.out.println("Got data: " +  getMethod.getResponseBodyAsString());
 *      } catch (ExecutionException e) {
 *         if (Throwables.getRootcause(e) instanceof SocketTimeoutException) {
 *           System.out.println("Timeout");
 *         }
 *      } finally {
 *        if (getMethod != null) {
 *          getMethod.releaseConnection();
 *        }
 *      }
 *    }
 * }
 * </pre>
 */
public class MultiHttpRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiHttpRequest.class);

  private final Executor _executor;
  // TODO: Verify that _connectionManager is an instaceOf MultithreadedHttpConnectionManager.
  //       SimpleHttpConnectionManager is not thread-safe.
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
  public CompletionService<MultiHttpRequestResponse> execute(List<String> urls,
      @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    return execute(urls, requestHeaders, timeoutMs, "GET", HttpGet::new);
  }

  /**
   * Execute certain http method on the urls in parallel using the executor service.
   * @param urls absolute URLs to execute the http method
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each http request
   * @param httpMethodName the name of the http method like GET, DELETE etc.
   * @param httpRequestBaseSupplier a function to create a new http method object.
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public <T extends HttpRequestBase> CompletionService<MultiHttpRequestResponse> execute(List<String> urls,
      @Nullable Map<String, String> requestHeaders, int timeoutMs, String httpMethodName,
      Function<String, T> httpRequestBaseSupplier) {
    // Create global request configuration
    RequestConfig defaultRequestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(timeoutMs)
        .setSocketTimeout(timeoutMs).build(); // setting the socket

    CloseableHttpClient client = HttpClients.custom().setConnectionManager(_connectionManager)
        .setDefaultRequestConfig(defaultRequestConfig).build(); // try-catch

    CompletionService<MultiHttpRequestResponse> completionService = new ExecutorCompletionService<>(_executor);
    for (String url : urls) {
      completionService.submit(() -> {
        try {
          T httpMethod = httpRequestBaseSupplier.apply(url);
          if (requestHeaders != null) {
            requestHeaders.forEach(((HttpRequestBase) httpMethod)::setHeader);
          }
          CloseableHttpResponse response = client.execute(httpMethod);
          return new MultiHttpRequestResponse(httpMethod.getURI(), response);
        } catch (Exception e) {
          // Log only exception type and message instead of the whole stack trace
          LOGGER.warn("Caught '{}' while executing: {} on URL: {}", e, httpMethodName, url);
          client.close();
          throw e;
        }
      });
    }
    return completionService;
  }
}
