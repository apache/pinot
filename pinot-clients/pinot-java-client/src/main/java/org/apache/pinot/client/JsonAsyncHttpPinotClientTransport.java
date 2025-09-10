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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ClientStats;
import org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder;
import org.asynchttpclient.Dsl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JSON encoded Pinot client transport over AsyncHttpClient.
 */
public class JsonAsyncHttpPinotClientTransport implements PinotClientTransport<ClientStats> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonAsyncHttpPinotClientTransport.class);
  private static final ObjectReader OBJECT_READER = JsonUtils.DEFAULT_READER;
  private static final String DEFAULT_EXTRA_QUERY_OPTION_STRING = "groupByMode=sql;responseFormat=sql";

  private final Map<String, String> _headers;
  private final String _scheme;

  private final int _brokerReadTimeout;
  private final AsyncHttpClient _httpClient;
  private final String _extraOptionStr;
  private final boolean _useMultistageEngine;

  public JsonAsyncHttpPinotClientTransport() {
    _brokerReadTimeout = 60000;
    _headers = new HashMap<>();
    _scheme = CommonConstants.HTTP_PROTOCOL;
    _extraOptionStr = DEFAULT_EXTRA_QUERY_OPTION_STRING;
    _httpClient = Dsl.asyncHttpClient(Dsl.config().setRequestTimeout(Duration.ofMillis(_brokerReadTimeout)));
    _useMultistageEngine = false;
  }

  public JsonAsyncHttpPinotClientTransport(Map<String, String> headers, String scheme, String extraOptionString,
      boolean useMultistageEngine, @Nullable SSLContext sslContext, ConnectionTimeouts connectionTimeouts,
      TlsProtocols tlsProtocols, @Nullable String appId) {
    _brokerReadTimeout = connectionTimeouts.getReadTimeoutMs();
    _headers = headers;
    _scheme = scheme;
    _extraOptionStr = StringUtils.isEmpty(extraOptionString) ? DEFAULT_EXTRA_QUERY_OPTION_STRING : extraOptionString;
    _useMultistageEngine = useMultistageEngine;

    Builder builder = Dsl.config();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.OPTIONAL));
    }

    builder.setRequestTimeout(Duration.ofMillis(_brokerReadTimeout))
        .setReadTimeout(Duration.ofMillis(connectionTimeouts.getReadTimeoutMs()))
        .setConnectTimeout(Duration.ofMillis(connectionTimeouts.getConnectTimeoutMs()))
        .setHandshakeTimeout(connectionTimeouts.getHandshakeTimeoutMs())
        .setUserAgent(ConnectionUtils.getUserAgentVersionFromClassPath("ua", appId))
        .setEnabledProtocols(tlsProtocols.getEnabledProtocols().toArray(new String[0]));
    _httpClient = Dsl.asyncHttpClient(builder.build());
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException {
    try {
      return executeQueryAsync(brokerAddress, query).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query) {
    try {
      ObjectNode json = JsonNodeFactory.instance.objectNode();
      json.put("sql", query);
      json.put("queryOptions", _extraOptionStr);

      LOGGER.debug("Query will use Multistage Engine = {}", _useMultistageEngine);

      String url = String.format("%s://%s%s", _scheme, brokerAddress, _useMultistageEngine ? "/query" : "/query/sql");
      BoundRequestBuilder requestBuilder = _httpClient.preparePost(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }
      LOGGER.debug("Sending query {} to {}", query, url);
      return requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").setBody(json.toString())
          .execute().toCompletableFuture().thenApply(httpResponse -> {
            LOGGER.debug("Completed query, HTTP status is {}", httpResponse.getStatusCode());

            if (httpResponse.getStatusCode() != 200) {
              throw new PinotClientException(
                  "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
            }

            try {
              return BrokerResponse.fromJson(OBJECT_READER.readTree(httpResponse.getResponseBodyAsStream()));
            } catch (IOException e) {
              throw new CompletionException(e);
            }
          });
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public void close()
      throws PinotClientException {
    if (_httpClient.isClosed()) {
      throw new PinotClientException("Connection is already closed!");
    }
    try {
      _httpClient.close();
    } catch (IOException exception) {
      throw new PinotClientException("Error while closing connection!");
    }
  }

  /**
   * Executes a query with cursor pagination support.
   *
   * @param brokerAddress The broker address in "host:port" format
   * @param query The SQL query to execute
   * @param numRows The number of rows to return in the first page
   * @return BrokerResponse containing the first page and cursor metadata
   * @throws PinotClientException If query execution fails
   */
  @Override
  public CursorAwareBrokerResponse executeQueryWithCursor(String brokerAddress, String query, int numRows)
      throws PinotClientException {
    try {
      return executeQueryWithCursorAsync(brokerAddress, query, numRows).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Executes a query asynchronously with cursor pagination support.
   *
   * @param brokerAddress The broker address in "host:port" format
   * @param query The SQL query to execute
   * @param numRows The number of rows to return in the first page
   * @return CompletableFuture containing BrokerResponse with first page and cursor metadata
   */
  @Override
  public CompletableFuture<CursorAwareBrokerResponse> executeQueryWithCursorAsync(String brokerAddress, String query,
      int numRows) {
    try {
      ObjectNode json = JsonNodeFactory.instance.objectNode();
      json.put("sql", query);
      json.put("queryOptions", _extraOptionStr);

      LOGGER.debug("Cursor query will use Multistage Engine = {}", _useMultistageEngine);

      String url = String.format("%s://%s%s?getCursor=true&numRows=%d", _scheme, brokerAddress,
          _useMultistageEngine ? "/query" : "/query/sql", numRows);
      BoundRequestBuilder requestBuilder = _httpClient.preparePost(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }
      LOGGER.debug("Sending cursor query {} to {}", query, url);
      return requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").setBody(json.toString())
          .execute().toCompletableFuture().thenApply(httpResponse -> {
            LOGGER.debug("Completed cursor query, HTTP status is {}", httpResponse.getStatusCode());

            if (httpResponse.getStatusCode() != 200) {
              throw new PinotClientException(
                  "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
            }

            try {
              return CursorAwareBrokerResponse.fromJson(OBJECT_READER.readTree(httpResponse.getResponseBodyAsStream()));
            } catch (IOException e) {
              throw new CompletionException(e);
            }
          });
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Fetches results from an existing cursor.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @param offset The starting row offset for the page
   * @param numRows The number of rows to fetch
   * @return BrokerResponse containing the requested page of results
   * @throws PinotClientException If fetch fails
   */
  public BrokerResponse fetchCursorResults(String brokerAddress, String requestId, int offset, int numRows)
      throws PinotClientException {
    try {
      return fetchCursorResultsAsync(brokerAddress, requestId, offset, numRows).get(_brokerReadTimeout,
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Fetches results from an existing cursor using offset-based pagination.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @param offset The starting row offset for the page
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing BrokerResponse with the requested page
   */
  public CompletableFuture<BrokerResponse> fetchCursorResultsAsync(String brokerAddress, String requestId, int offset,
      int numRows) {
    try {
      String url = String.format("%s://%s/responseStore/%s/results?offset=%d&numRows=%d", _scheme, brokerAddress,
          requestId, offset, numRows);
      BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }
      LOGGER.debug("Fetching cursor results from {}", url);
      return requestBuilder.execute().toCompletableFuture().thenApply(httpResponse -> {
        LOGGER.debug("Completed cursor fetch, HTTP status is {}", httpResponse.getStatusCode());

        if (httpResponse.getStatusCode() != 200) {
          throw new PinotClientException(
              "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
        }

        try {
          return BrokerResponse.fromJson(OBJECT_READER.readTree(httpResponse.getResponseBodyAsStream()));
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      });
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Retrieves metadata for an existing cursor.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @return BrokerResponse containing cursor metadata
   * @throws PinotClientException If metadata retrieval fails
   */
  public BrokerResponse getCursorMetadata(String brokerAddress, String requestId) throws PinotClientException {
    try {
      return getCursorMetadataAsync(brokerAddress, requestId).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Retrieves metadata for an existing cursor asynchronously.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @return CompletableFuture containing BrokerResponse with cursor metadata
   */
  public CompletableFuture<BrokerResponse> getCursorMetadataAsync(String brokerAddress, String requestId) {
    try {
      String url = String.format("%s://%s/responseStore/%s/", _scheme, brokerAddress, requestId);
      BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }
      LOGGER.debug("Getting cursor metadata from {}", url);
      return requestBuilder.execute().toCompletableFuture().thenApply(httpResponse -> {
        LOGGER.debug("Completed cursor metadata fetch, HTTP status is {}", httpResponse.getStatusCode());

        if (httpResponse.getStatusCode() != 200) {
          throw new PinotClientException(
              "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
        }

        try {
          return BrokerResponse.fromJson(OBJECT_READER.readTree(httpResponse.getResponseBodyAsStream()));
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      });
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Deletes a cursor and cleans up its resources.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor to delete
   * @throws PinotClientException If cursor deletion fails
   */
  public void deleteCursor(String brokerAddress, String requestId) throws PinotClientException {
    try {
      deleteCursorAsync(brokerAddress, requestId).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Deletes a cursor and cleans up its resources asynchronously.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor to delete
   * @return CompletableFuture that completes when the cursor is deleted
   */
  public CompletableFuture<Void> deleteCursorAsync(String brokerAddress, String requestId) {
    try {
      String url = String.format("%s://%s/responseStore/%s/", _scheme, brokerAddress, requestId);
      BoundRequestBuilder requestBuilder = _httpClient.prepareDelete(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }
      LOGGER.debug("Deleting cursor at {}", url);
      return requestBuilder.execute().toCompletableFuture().thenApply(httpResponse -> {
        LOGGER.debug("Completed cursor deletion, HTTP status is {}", httpResponse.getStatusCode());

        if (httpResponse.getStatusCode() != 200) {
          throw new PinotClientException(
              "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
        }
        return null;
      });
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public CursorAwareBrokerResponse fetchNextPage(String brokerAddress, String cursorId) throws PinotClientException {
    try {
      return fetchNextPageAsync(brokerAddress, cursorId).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public CompletableFuture<CursorAwareBrokerResponse> fetchNextPageAsync(String brokerAddress, String cursorId) {
    String url = String.format("%s://%s/responseStore/%s/next", _scheme, brokerAddress, cursorId);
    return executeCursorRequest(url);
  }

  @Override
  public CursorAwareBrokerResponse fetchPreviousPage(String brokerAddress, String cursorId)
      throws PinotClientException {
    try {
      return fetchPreviousPageAsync(brokerAddress, cursorId).get(_brokerReadTimeout,
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public CompletableFuture<CursorAwareBrokerResponse> fetchPreviousPageAsync(String brokerAddress, String cursorId) {
    String url = String.format("%s://%s/responseStore/%s/previous", _scheme, brokerAddress, cursorId);
    return executeCursorRequest(url);
  }

  @Override
  public CursorAwareBrokerResponse seekToPage(String brokerAddress, String cursorId, int pageNumber)
      throws PinotClientException {
    try {
      return seekToPageAsync(brokerAddress, cursorId, pageNumber).get(_brokerReadTimeout,
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public CompletableFuture<CursorAwareBrokerResponse> seekToPageAsync(String brokerAddress,
      String cursorId, int pageNumber) {
    String url = String.format("%s://%s/responseStore/%s/seek/%d", _scheme, brokerAddress, cursorId,
        pageNumber);
    return executeCursorRequest(url);
  }

  /**
   * Helper method to execute cursor navigation requests.
   */
  private CompletableFuture<CursorAwareBrokerResponse> executeCursorRequest(String url) {
    BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);

    if (_headers != null) {
      _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
    }

    LOGGER.debug("Sending cursor request to {}", url);
    return requestBuilder.execute().toCompletableFuture().thenApply(httpResponse -> {
      LOGGER.debug("Completed cursor request, HTTP status is {}", httpResponse.getStatusCode());

      if (httpResponse.getStatusCode() != 200) {
        throw new PinotClientException(
            "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
      }

      try {
        JsonNode brokerResponse = OBJECT_READER.readTree(httpResponse.getResponseBody());
        return CursorAwareBrokerResponse.fromJson(brokerResponse);
      } catch (Exception e) {
        throw new PinotClientException("Unable to parse broker response", e);
      }
    });
  }

  @Override
  public ClientStats getClientMetrics() {
    return _httpClient.getClientStats();
  }
}
