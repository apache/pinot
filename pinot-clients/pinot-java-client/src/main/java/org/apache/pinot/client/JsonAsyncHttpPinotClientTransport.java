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

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JSON encoded Pinot client transport over AsyncHttpClient.
 */
public class JsonAsyncHttpPinotClientTransport implements PinotClientTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonAsyncHttpPinotClientTransport.class);
  private static final ObjectReader OBJECT_READER = JsonUtils.DEFAULT_READER;
  private static final String DEFAULT_EXTRA_QUERY_OPTION_STRING = "groupByMode=sql;responseFormat=sql";

  private final Map<String, String> _headers;
  private final String _scheme;

  private final int _brokerReadTimeout;
  private final AsyncHttpClient _httpClient;
  private final String _extraOptionStr;

  public JsonAsyncHttpPinotClientTransport() {
    _brokerReadTimeout = 60000;
    _headers = new HashMap<>();
    _scheme = CommonConstants.HTTP_PROTOCOL;
    _extraOptionStr = DEFAULT_EXTRA_QUERY_OPTION_STRING;
    _httpClient = Dsl.asyncHttpClient();
  }

  public JsonAsyncHttpPinotClientTransport(Map<String, String> headers, String scheme, String extraOptionString,
      @Nullable SSLContext sslContext, ConnectionTimeouts connectionTimeouts, TlsProtocols tlsProtocols,
      @Nullable String appId) {
    _brokerReadTimeout = connectionTimeouts.getReadTimeoutMs();
    _headers = headers;
    _scheme = scheme;
    _extraOptionStr = StringUtils.isEmpty(extraOptionString) ? DEFAULT_EXTRA_QUERY_OPTION_STRING : extraOptionString;

    Builder builder = Dsl.config();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.OPTIONAL));
    }

    builder.setReadTimeout(connectionTimeouts.getReadTimeoutMs())
        .setConnectTimeout(connectionTimeouts.getConnectTimeoutMs())
        .setHandshakeTimeout(connectionTimeouts.getHandshakeTimeoutMs())
        .setUserAgent(ConnectionUtils.getUserAgentVersionFromClassPath("ua", appId))
        .setEnabledProtocols(tlsProtocols.getEnabledProtocols().toArray(new String[0]));
    _httpClient = Dsl.asyncHttpClient(builder.build());
  }

  public JsonAsyncHttpPinotClientTransport(Map<String, String> headers, String scheme, String extraOptionStr,
      @Nullable SslContext sslContext, ConnectionTimeouts connectionTimeouts, TlsProtocols tlsProtocols,
      @Nullable String appId) {
    _brokerReadTimeout = connectionTimeouts.getReadTimeoutMs();
    _headers = headers;
    _scheme = scheme;
    _extraOptionStr = StringUtils.isEmpty(extraOptionStr) ? DEFAULT_EXTRA_QUERY_OPTION_STRING : extraOptionStr;

    Builder builder = Dsl.config();
    if (sslContext != null) {
      builder.setSslContext(sslContext);
    }

    builder.setReadTimeout(connectionTimeouts.getReadTimeoutMs())
        .setConnectTimeout(connectionTimeouts.getConnectTimeoutMs())
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
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, String query) {
    try {
      ObjectNode json = JsonNodeFactory.instance.objectNode();
      json.put("sql", query);
      json.put("queryOptions", _extraOptionStr);

      String url = _scheme + "://" + brokerAddress + "/query/sql";
      BoundRequestBuilder requestBuilder = _httpClient.preparePost(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      Future<Response> response =
          requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").setBody(json.toString())
              .execute();
      return new BrokerResponseFuture(response, query, url, _brokerReadTimeout);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, Request request)
      throws PinotClientException {
    try {
      return executeQueryAsync(brokerAddress, request).get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
      throws PinotClientException {
    return executeQueryAsync(brokerAddress, request.getQuery());
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

  private static class BrokerResponseFuture implements Future<BrokerResponse> {
    private final Future<Response> _response;
    private final String _query;
    private final String _url;
    private final long _brokerReadTimeout;

    public BrokerResponseFuture(Future<Response> response, String query, String url, long brokerReadTimeout) {
      _response = response;
      _query = query;
      _url = url;
      _brokerReadTimeout = brokerReadTimeout;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return _response.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return _response.isCancelled();
    }

    @Override
    public boolean isDone() {
      return _response.isDone();
    }

    @Override
    public BrokerResponse get()
        throws ExecutionException {
      return get(_brokerReadTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public BrokerResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      try {
        LOGGER.debug("Sending query {} to {}", _query, _url);

        Response httpResponse = _response.get(timeout, unit);

        LOGGER.debug("Completed query, HTTP status is {}", httpResponse.getStatusCode());

        if (httpResponse.getStatusCode() != 200) {
          throw new PinotClientException(
              "Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
        }

        String responseBody = httpResponse.getResponseBody(StandardCharsets.UTF_8);
        return BrokerResponse.fromJson(OBJECT_READER.readTree(responseBody));
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }
  }
}
