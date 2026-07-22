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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
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

  /// Executes cancellable GET requests that buffer each response body before releasing its connection.
  public BufferedRequestExecution executeGetBuffered(List<String> urls, @Nullable Map<String, String> requestHeaders,
      int timeoutMs) {
    BufferedRequestExecution execution = createBufferedExecution(timeoutMs);
    try {
      for (String url : urls) {
        HttpGet request = new HttpGet(url);
        if (requestHeaders != null) {
          requestHeaders.forEach(request::setHeader);
        }
        execution.submit(request);
      }
      return execution;
    } catch (RuntimeException | Error e) {
      execution.close();
      throw e;
    }
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
  public <T extends HttpUriRequestBase> CompletionService<MultiHttpRequestResponse> execute(
      List<Pair<String, String>> urlsAndRequestBodies, @Nullable Map<String, String> requestHeaders, int timeoutMs,
      String httpMethodName, Function<String, T> httpRequestBaseSupplier) {
    // Create global request configuration
    Timeout timeout = Timeout.of(timeoutMs, TimeUnit.MILLISECONDS);
    RequestConfig defaultRequestConfig =
        RequestConfig.custom().setConnectionRequestTimeout(timeout).setResponseTimeout(timeout)
            .build(); // setting the socket

    HttpClientBuilder httpClientBuilder =
        HttpClients.custom().setConnectionManager(_connectionManager).setDefaultRequestConfig(defaultRequestConfig);

    CompletionService<MultiHttpRequestResponse> completionService = new ExecutorCompletionService<>(_executor);
    CloseableHttpClient client = httpClientBuilder.build();
    for (Pair<String, String> pair : urlsAndRequestBodies) {
      String url = pair.getLeft();
      String body = pair.getRight();
      HttpUriRequestBase httpMethod = httpRequestBaseSupplier.apply(url);
      // If the http method is POST, set the request body
      if (httpMethod instanceof HttpPost) {
        ((HttpPost) httpMethod).setEntity(new StringEntity(body));
      }
      if (requestHeaders != null) {
        requestHeaders.forEach(httpMethod::setHeader);
      }
      completionService.submit(() -> {
        CloseableHttpResponse response = null;
        boolean responseHandedOff = false;
        try {
          response = client.execute(httpMethod);
          httpMethod.setAbsoluteRequestUri(true);
          MultiHttpRequestResponse result =
              new MultiHttpRequestResponse(URI.create(httpMethod.getRequestUri()), response);
          responseHandedOff = true;
          return result;
        } finally {
          if (response != null && !responseHandedOff) {
            response.close();
          }
        }
      });
    }
    return completionService;
  }

  /// Creates a cancellable execution that buffers each response body before releasing its connection. Callers may
  /// submit requests with per-request configuration and must close the execution when collection finishes.
  public BufferedRequestExecution createBufferedExecution(int timeoutMs) {
    Timeout timeout = Timeout.of(timeoutMs, TimeUnit.MILLISECONDS);
    RequestConfig defaultRequestConfig =
        RequestConfig.custom().setConnectionRequestTimeout(timeout).setResponseTimeout(timeout).build();
    CloseableHttpClient client = HttpClients.custom().setConnectionManager(_connectionManager)
        .setConnectionManagerShared(true).setDefaultRequestConfig(defaultRequestConfig).build();
    return new BufferedRequestExecution(new ExecutorCompletionService<>(_executor), client);
  }

  private static MultiHttpRequestBufferedResponse executeBufferedRequest(CloseableHttpClient client,
      HttpUriRequestBase httpMethod)
      throws Exception {
    try (CloseableHttpResponse response = client.execute(httpMethod)) {
      httpMethod.setAbsoluteRequestUri(true);
      URI uri = URI.create(httpMethod.getRequestUri());
      return new MultiHttpRequestBufferedResponse(uri, response.getCode(), response.getReasonPhrase(),
          EntityUtils.toString(response.getEntity()));
    } catch (Exception e) {
      throw new IOException("Failed to execute request on URL: " + httpMethod.getRequestUri() + ": "
          + e.getMessage(), e);
    }
  }

  /// Mutable execution state for buffered requests. A single collector thread must poll it and close it when done.
  /// Closing cancels every unfinished request, including when collection exits early.
  public static final class BufferedRequestExecution implements AutoCloseable {
    private final CompletionService<MultiHttpRequestBufferedResponse> _completionService;
    private final CloseableHttpClient _client;
    private final Map<Future<MultiHttpRequestBufferedResponse>, HttpUriRequestBase> _pendingRequests = new HashMap<>();
    private boolean _closed;

    private BufferedRequestExecution(CompletionService<MultiHttpRequestBufferedResponse> completionService,
        CloseableHttpClient client) {
      _completionService = completionService;
      _client = client;
    }

    /// Submits one request and returns its future. The request is cancelled automatically when this execution closes.
    public Future<MultiHttpRequestBufferedResponse> submit(HttpUriRequestBase request) {
      if (_closed) {
        throw new IllegalStateException("Buffered request execution is closed");
      }
      try {
        Future<MultiHttpRequestBufferedResponse> future =
            _completionService.submit(() -> executeBufferedRequest(_client, request));
        _pendingRequests.put(future, request);
        return future;
      } catch (RuntimeException | Error e) {
        request.cancel();
        throw e;
      }
    }

    /// Waits for one completed request. The request's own HTTP timeout remains the only timeout applied.
    public Future<MultiHttpRequestBufferedResponse> take()
        throws InterruptedException {
      if (_closed) {
        throw new IllegalStateException("Buffered request execution is closed");
      }
      Future<MultiHttpRequestBufferedResponse> completedFuture = _completionService.take();
      _pendingRequests.remove(completedFuture);
      return completedFuture;
    }

    /// Waits up to the supplied timeout for one completed request.
    @Nullable
    public Future<MultiHttpRequestBufferedResponse> poll(long timeout, TimeUnit unit)
        throws InterruptedException {
      if (_closed) {
        throw new IllegalStateException("Buffered request execution is closed");
      }
      Future<MultiHttpRequestBufferedResponse> completedFuture = _completionService.poll(timeout, unit);
      if (completedFuture != null) {
        _pendingRequests.remove(completedFuture);
      }
      return completedFuture;
    }

    /// Cancels one pending request. Calling this for a request that already completed is harmless.
    public void cancel(Future<MultiHttpRequestBufferedResponse> future) {
      HttpUriRequestBase request = _pendingRequests.remove(future);
      if (request != null) {
        request.cancel();
      }
      future.cancel(true);
    }

    /// Aborts unfinished HTTP requests and cancels their executor tasks. This method is idempotent.
    @Override
    public void close() {
      if (_closed) {
        return;
      }
      _closed = true;
      for (Map.Entry<Future<MultiHttpRequestBufferedResponse>, HttpUriRequestBase> entry
          : new ArrayList<>(_pendingRequests.entrySet())) {
        cancel(entry.getKey());
      }
      try {
        _client.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close buffered HTTP client: {}", e.getMessage());
      }
    }
  }
}
