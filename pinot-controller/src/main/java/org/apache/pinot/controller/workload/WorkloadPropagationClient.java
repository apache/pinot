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
package org.apache.pinot.controller.workload;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class responsible for HTTP communication with brokers and servers for workload propagation.
 * Handles request building, sending, retries, and endpoint discovery.
 * Encapsulates async HTTP client configuration and lifecycle management.
 */
public class WorkloadPropagationClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadPropagationClient.class);

  private static final int PROPAGATION_TIMEOUT_SECONDS = 300;
  private static final int WORKLOAD_PROPAGATION_MAX_RETRIES = 3;
  private static final long RETRY_INITIAL_DELAY_MS = 3000L;
  private static final double RETRY_BACKOFF_MULTIPLIER = 2.0;
  private static final String WORKLOAD_HTTP_THREAD_FORMAT = "workload-propagation-http-%d";

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final CloseableHttpAsyncClient _asyncHttpClient;
  private final ExecutorService _httpCallbackExecutor;  // Dedicated executor for HTTP I/O callbacks
  private final RateLimiter _rateLimiter;

  private Pair<String, Integer> _serverHttpSchemeAndPort;
  private Pair<String, Integer> _brokerHttpSchemeAndPort;

  private final ControllerMetrics _controllerMetrics;

  public WorkloadPropagationClient(PinotHelixResourceManager pinotHelixResourceManager,
                                   ControllerConf controllerConf,
                                   ControllerMetrics controllerMetrics) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    // Create dedicated executor for HTTP callback processing to avoid blocking workload propagation tasks
    int httpThreads = controllerConf.getControllerWorkloadExecutorThreads();
    int httpQueueSize = controllerConf.getControllerWorkloadExecutorQueueSize();
    _httpCallbackExecutor = new ThreadPoolExecutor(httpThreads, httpThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(httpQueueSize),
        new ThreadFactoryBuilder().setNameFormat(WORKLOAD_HTTP_THREAD_FORMAT).setDaemon(true).build(),
        new ThreadPoolExecutor.AbortPolicy());
    double qps = controllerConf.getControllerWorkloadPropagationRequestsPerSecond();
    _rateLimiter = RateLimiter.create(qps);
    // Initialize async HTTP client with configuration
    _asyncHttpClient = createAsyncHttpClient(AsyncHttpClientConfig.fromPinotConfig(controllerConf));
    // Discover and cache endpoint configurations at startup
    Map<NodeConfig.Type, Pair<String, Integer>> httpSchemeAndPort = discoverHttpSchemesAndPort(null);
    _serverHttpSchemeAndPort = httpSchemeAndPort.getOrDefault(NodeConfig.Type.SERVER_NODE, null);
    _brokerHttpSchemeAndPort = httpSchemeAndPort.getOrDefault(NodeConfig.Type.BROKER_NODE, null);
    _controllerMetrics = controllerMetrics;
    LOGGER.info("Initialized WorkloadPropagationClient with QPS: {}, HTTP threads: {}, queue size: {}, "
        + "server endpoint: {}, broker endpoint: {}", qps, httpThreads, httpQueueSize, _serverHttpSchemeAndPort,
        _brokerHttpSchemeAndPort);
  }

  private CloseableHttpAsyncClient createAsyncHttpClient(AsyncHttpClientConfig config) {
    // Configure I/O reactor with specified thread count
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setIoThreadCount(config._ioThreadCount)
        .build();

    // Build async connection manager with TLS support and connection pool settings
    PoolingAsyncClientConnectionManager asyncConnectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
        .setTlsStrategy(ClientTlsStrategyBuilder.create()
            .setSslContext(TlsUtils.getSslContext())
            .build())
        .setMaxConnTotal(config._maxConnTotal)
        .setMaxConnPerRoute(config._maxConnPerRoute)
        .build();

    // Configure request timeouts
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(Timeout.ofMilliseconds(config._requestTimeoutMs))
        .setConnectTimeout(Timeout.ofMilliseconds(config._connectionTimeoutMs))
        .setResponseTimeout(Timeout.ofMilliseconds(config._socketTimeoutMs))
        .build();

    CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setConnectionManager(asyncConnectionManager)
        .setIOReactorConfig(ioReactorConfig)
        .setDefaultRequestConfig(requestConfig)
        .build();
    client.start();

    LOGGER.info("Started WorkloadPropagationClient HTTP client: ioThreads={}, maxConnTotal={}, maxConnPerRoute={}, "
            + "connectionTimeoutMs={}, socketTimeoutMs={}, requestTimeoutMs={}",
        config._ioThreadCount, config._maxConnTotal, config._maxConnPerRoute,
        config._connectionTimeoutMs, config._socketTimeoutMs, config._requestTimeoutMs);

    return client;
  }

  /**
   * Sends workload refresh messages to instances via HTTP in parallel.
   * Uses dedicated executor pool for callback processing and RateLimiter to control QPS.
   */
  public void sendQueryWorkloadMessage(Map<String, QueryWorkloadRequest> instanceToRefreshRequestMap) {
    long startTime = System.currentTimeMillis();
    int totalInstances = instanceToRefreshRequestMap.size();

    LOGGER.info("Total {} instances to send workload refresh message", totalInstances);
    // Create async requests for all instances (callbacks processed by dedicated executor)
    List<CompletableFuture<Boolean>> futures = new ArrayList<>(totalInstances);
    for (Map.Entry<String, QueryWorkloadRequest> entry : instanceToRefreshRequestMap.entrySet()) {
      String instance = entry.getKey();
      try {
        QueryWorkloadRequest queryWorkloadRequest = entry.getValue();
        String baseUrl = InstanceTypeUtils.isBroker(instance) ? getBrokerUrl(instance) : getServerUrl(instance);
        String url = baseUrl + "/queryWorkloadConfigs";
        // Acquire rate limit permit before initiating async request
        _rateLimiter.acquire();
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_MESSAGES_COUNT, 1);
        // Send async request with retry logic (callbacks processed on _httpCallbackExecutor)
        SimpleHttpRequest httpRequest;
        if (queryWorkloadRequest.isRefresh()) {
          String requestBody = JsonUtils.objectToString(queryWorkloadRequest.getWorkloadToCostMap());
          httpRequest = SimpleRequestBuilder.post(url).setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
              .setBody(requestBody, ContentType.APPLICATION_JSON).build();
        } else {
          String deleteUrl = url + "?workloadNames=" + String.join(",",
              queryWorkloadRequest.getWorkloadToCostMap().keySet());
          httpRequest = SimpleRequestBuilder.delete(deleteUrl).build();
        }
        CompletableFuture<Boolean> future = sendWorkloadRequestWithRetry(httpRequest, instance);
        futures.add(future);
      } catch (Exception e) {
        LOGGER.error("Error creating request for instance: {}", instance, e);
        futures.add(CompletableFuture.completedFuture(false));
      }
    }

    // Wait for all requests to complete with timeout
    int successCount = 0;
    int failureCount = 0;
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(PROPAGATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOGGER.warn("Query workload refresh timed out after {}s", PROPAGATION_TIMEOUT_SECONDS);
      futures.forEach(f -> f.cancel(true));
    } catch (Exception e) {
      LOGGER.error("Error waiting for query workload refresh completion", e);
      futures.forEach(f -> f.cancel(true));
    }
    // Count successes and failures
    for (CompletableFuture<Boolean> future : futures) {
      if (future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally() && future.join()) {
        successCount++;
      } else {
        failureCount++;
      }
    }
    if (failureCount > 0) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_MESSAGES_FAILED, failureCount);
    }
    _controllerMetrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_SEND_MESSAGE_TIME_MS,
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    if (failureCount > 0) {
      LOGGER.warn("Query workload refresh completed with failures: {}/{} successful", successCount, totalInstances);
    } else {
      LOGGER.info("Query workload refresh completed successfully: {}/{} instances", successCount, totalInstances);
    }
  }

  private String getServerUrl(String instanceName) {
    if (_serverHttpSchemeAndPort == null) {
      _serverHttpSchemeAndPort = discoverHttpSchemesAndPort(NodeConfig.Type.SERVER_NODE)
          .getOrDefault(NodeConfig.Type.SERVER_NODE, null);
    }
    return InstanceUtils.getInstanceBaseUri(instanceName, _serverHttpSchemeAndPort.getKey(),
        _serverHttpSchemeAndPort.getRight());
  }

  private String getBrokerUrl(String instanceName) {
    if (_brokerHttpSchemeAndPort == null) {
      _brokerHttpSchemeAndPort = discoverHttpSchemesAndPort(NodeConfig.Type.BROKER_NODE)
          .getOrDefault(NodeConfig.Type.BROKER_NODE, null);
    }
    return InstanceUtils.getInstanceBaseUri(instanceName, _brokerHttpSchemeAndPort.getKey(),
        _brokerHttpSchemeAndPort.getRight());
  }

  /**
   * Discovers endpoint configurations (protocol scheme and port) for node types.
   * @param targetType Specific node type to discover, or null to discover both broker and server
   * @return Map of node type to endpoint config (scheme, port)
   */
  private Map<NodeConfig.Type, Pair<String, Integer>> discoverHttpSchemesAndPort(
      @Nullable NodeConfig.Type targetType) {
    List<NodeConfig.Type> typesToDiscover = targetType == null
        ? List.of(NodeConfig.Type.BROKER_NODE, NodeConfig.Type.SERVER_NODE)
        : List.of(targetType);
    List<String> instances = new ArrayList<>();
    if (targetType == NodeConfig.Type.BROKER_NODE) {
      instances.addAll(_pinotHelixResourceManager.getAllBrokerInstances());
    } else if (targetType == NodeConfig.Type.SERVER_NODE) {
      instances.addAll(_pinotHelixResourceManager.getAllServerInstances());
    } else {
      instances.addAll(_pinotHelixResourceManager.getAllInstances());
    }
    Map<NodeConfig.Type, Pair<String, Integer>> httpSchemesAndPort = new HashMap<>();
    try {
      for (String instanceName : instances) {
        if (httpSchemesAndPort.size() == typesToDiscover.size()) {
          break;
        }
        NodeConfig.Type matchedType = null;
        for (NodeConfig.Type type : typesToDiscover) {
          if (!httpSchemesAndPort.containsKey(type)
              && ((type == NodeConfig.Type.BROKER_NODE && InstanceTypeUtils.isBroker(instanceName))
              || (type == NodeConfig.Type.SERVER_NODE && InstanceTypeUtils.isServer(instanceName)))) {
            matchedType = type;
            break;
          }
        }
        if (matchedType == null) {
          continue;
        }
        InstanceConfig config = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
        if (config != null) {
          Pair<String, Integer> endpointConfig = InstanceUtils.extractHttpSchemeAndPort(config);
          if (endpointConfig != null) {
            httpSchemesAndPort.put(matchedType, endpointConfig);
            LOGGER.info("Discovered {} endpoint config: {}:{}", matchedType, endpointConfig.getLeft(),
                endpointConfig.getRight());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Endpoint config discovery error: {}", e.getMessage());
    }
    return httpSchemesAndPort;
  }

  /**
   * Sends a workload refresh request with automatic retries and exponential backoff.
   * Uses async HTTP client for non-blocking I/O with callbacks processed on dedicated executor.
   */
  private CompletableFuture<Boolean> sendWorkloadRequestWithRetry(SimpleHttpRequest request, String instanceId) {
    return sendWorkloadRequestWithRetryInternal(request, instanceId, 0);
  }

  private CompletableFuture<Boolean> sendWorkloadRequestWithRetryInternal(SimpleHttpRequest request,
      String instanceId, int attemptNumber) {
    return sendWorkloadRequestAsync(request, instanceId)
        .thenComposeAsync(success -> {
          if (success || attemptNumber >= WORKLOAD_PROPAGATION_MAX_RETRIES - 1) {
            return CompletableFuture.completedFuture(success);
          }
          // Retry with exponential backoff (non-retriable errors throw exception, not reach here)
          long delayMs = (long) (RETRY_INITIAL_DELAY_MS * Math.pow(RETRY_BACKOFF_MULTIPLIER, attemptNumber));
          LOGGER.debug("Retrying workload request for instance {} after {}ms delay (attempt {}/{})",
              instanceId, delayMs, attemptNumber + 1, WORKLOAD_PROPAGATION_MAX_RETRIES);
          CompletableFuture<Boolean> delayed = new CompletableFuture<>();
          CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, _httpCallbackExecutor)
              .execute(() -> sendWorkloadRequestWithRetryInternal(request, instanceId, attemptNumber + 1)
                  .whenComplete((result, error) -> {
                    if (error != null) {
                      delayed.completeExceptionally(error);
                    } else {
                      delayed.complete(result);
                    }
                  }));
          return delayed;
        }, _httpCallbackExecutor)
        .exceptionally(ex -> {
          Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
          // Non-retriable errors (400/403/404) should not be retried - return false to count as failure
          if (cause instanceof IllegalStateException && cause.getCause() instanceof HttpErrorStatusException) {
            HttpErrorStatusException httpEx = (HttpErrorStatusException) cause.getCause();
            if (isNonRetriableStatusCode(httpEx.getStatusCode())) {
              return false;
            }
          }
          // Other exceptions should have been retried already or hit max retries
          LOGGER.error("Unexpected exception in retry logic for instance: {}", instanceId, ex);
          return false;
        });
  }

  private CompletableFuture<Boolean> sendWorkloadRequestAsync(SimpleHttpRequest request, String instanceId) {
    CompletableFuture<SimpleHttpResponse> httpFuture = sendHttpRequestAsync(request);

    return httpFuture.thenApplyAsync(response -> {
      try {
        SimpleHttpResponse wrappedResponse = HttpClient.wrapAndThrowHttpException(response);
        if (wrappedResponse.getStatusCode() == HttpStatus.SC_OK
            || wrappedResponse.getStatusCode() == HttpStatus.SC_ACCEPTED) {
          LOGGER.info("Successfully sent workload request to instance: {}", instanceId);
          return true;
        }
        return false;
      } catch (Exception e) {
        if (e instanceof HttpErrorStatusException) {
          HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e;
          // Non-retriable errors - wrap in unchecked exception to stop retries and count as failure
          if (isNonRetriableStatusCode(httpErrorStatusException.getStatusCode())) {
            LOGGER.info("Non-retriable error while sending query workload configs from controller,"
                + " Instance: {}, status code: {}", instanceId, httpErrorStatusException.getStatusCode());
            // Wrap checked exception in IllegalStateException (unchecked) to propagate through CompletableFuture
            throw new IllegalStateException("Non-retriable HTTP error: " + httpErrorStatusException.getStatusCode(),
                httpErrorStatusException);
          }
        }
        return false;
      }
    }, _httpCallbackExecutor);
  }

  /**
   * Checks if the HTTP status code indicates a non-retriable error.
   * Non-retriable errors: 400 (Bad Request), 403 (Forbidden), 404 (Not Found)
   */
  private boolean isNonRetriableStatusCode(int statusCode) {
    return statusCode == HttpStatus.SC_BAD_REQUEST
        || statusCode == HttpStatus.SC_FORBIDDEN || statusCode == HttpStatus.SC_NOT_FOUND;
  }

  /**
   * Sends an HTTP request asynchronously using non-blocking I/O.
   */
  private CompletableFuture<SimpleHttpResponse> sendHttpRequestAsync(SimpleHttpRequest request) {
    CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();

    try {
      Future<?> httpFuture = _asyncHttpClient.execute(request, new FutureCallback<>() {
        @Override
        public void completed(org.apache.hc.client5.http.async.methods.SimpleHttpResponse response) {
          try {
            future.complete(new SimpleHttpResponse(response.getCode(),
              response.getBodyText() != null ? response.getBodyText() : ""));
          } catch (Exception e) {
            future.completeExceptionally(new IOException("Failed to process async response", e));
          }
        }

        @Override
        public void failed(Exception ex) {
          future.completeExceptionally(new IOException("Async HTTP request failed", ex));
        }

        @Override
        public void cancelled() {
          future.completeExceptionally(new IOException("Async HTTP request was cancelled"));
        }
      });

      // If CompletableFuture is cancelled, cancel the underlying HTTP request
      future.whenComplete((result, error) -> {
        if (future.isCancelled()) {
          httpFuture.cancel(true);
        }
      });
    } catch (Exception e) {
      future.completeExceptionally(new IOException("Failed to initiate async HTTP request", e));
    }
    return future;
  }

  @Override
  public void close() {
    try {
      _asyncHttpClient.close();
      LOGGER.info("Closed WorkloadPropagationClient HTTP client");
    } catch (IOException e) {
      LOGGER.error("Error closing WorkloadPropagationClient HTTP client", e);
    }
  }

  /**
   * HTTP client configuration for workload propagation.
   */
  private static class AsyncHttpClientConfig {
    private static final String MAX_CONNS_CONFIG_NAME = "workload.async.http.client.maxConnTotal";
    private static final String MAX_CONNS_PER_ROUTE_CONFIG_NAME = "workload.async.http.client.maxConnPerRoute";
    private static final String CONNECTION_TIMEOUT_CONFIG_NAME = "workload.async.http.client.connectionTimeoutMs";
    private static final String SOCKET_TIMEOUT_CONFIG_NAME = "workload.async.http.client.socketTimeoutMs";
    private static final String REQUEST_TIMEOUT_CONFIG_NAME = "workload.async.http.client.requestTimeoutMs";
    private static final String IO_THREAD_COUNT_CONFIG_NAME = "workload.async.http.client.ioThreadCount";

    private static final int DEFAULT_MAX_CONN_TOTAL = 1000;
    private static final int DEFAULT_MAX_CONN_PER_ROUTE = 2;
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 60_000;
    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 60_000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 60_000;
    private static final int DEFAULT_IO_THREAD_COUNT = 1;

    final int _maxConnTotal;
    final int _maxConnPerRoute;
    final int _connectionTimeoutMs;
    final int _socketTimeoutMs;
    final int _requestTimeoutMs;
    final int _ioThreadCount;

    AsyncHttpClientConfig(int maxConnTotal, int maxConnPerRoute, int connectionTimeoutMs,
                          int socketTimeoutMs, int requestTimeoutMs, int ioThreadCount) {
      _maxConnTotal = maxConnTotal;
      _maxConnPerRoute = maxConnPerRoute;
      _connectionTimeoutMs = connectionTimeoutMs;
      _socketTimeoutMs = socketTimeoutMs;
      _requestTimeoutMs = requestTimeoutMs;
      _ioThreadCount = ioThreadCount;
    }

    static AsyncHttpClientConfig fromPinotConfig(PinotConfiguration config) {
      int maxConnTotal = DEFAULT_MAX_CONN_TOTAL;
      int maxConnPerRoute = DEFAULT_MAX_CONN_PER_ROUTE;
      int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
      int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
      int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
      int ioThreadCount = DEFAULT_IO_THREAD_COUNT;

      String value = config.getProperty(MAX_CONNS_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        maxConnTotal = Integer.parseInt(value);
      }
      value = config.getProperty(MAX_CONNS_PER_ROUTE_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        maxConnPerRoute = Integer.parseInt(value);
      }
      value = config.getProperty(CONNECTION_TIMEOUT_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        connectionTimeoutMs = Integer.parseInt(value);
      }
      value = config.getProperty(SOCKET_TIMEOUT_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        socketTimeoutMs = Integer.parseInt(value);
      }
      value = config.getProperty(REQUEST_TIMEOUT_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        requestTimeoutMs = Integer.parseInt(value);
      }
      value = config.getProperty(IO_THREAD_COUNT_CONFIG_NAME);
      if (StringUtils.isNotEmpty(value)) {
        ioThreadCount = Integer.parseInt(value);
      }
      return new AsyncHttpClientConfig(maxConnTotal, maxConnPerRoute, connectionTimeoutMs,
          socketTimeoutMs, requestTimeoutMs, ioThreadCount);
    }
  }
}
