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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JSON encoded Pinot client transport over AsyncHttpClient.
 */
public class JsonAsyncHttpPinotClientTransport implements PinotClientTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonAsyncHttpPinotClientTransport.class);
  private static final ObjectReader OBJECT_READER = new ObjectMapper().reader();

  AsyncHttpClient _httpClient;

  Map<String, String> _headers = new HashMap<>();
  String _scheme = CommonConstants.HTTP_PROTOCOL;
  SSLContext _sslContext = null;

  public JsonAsyncHttpPinotClientTransport() {
    _httpClient = new AsyncHttpClient();
  }

  public JsonAsyncHttpPinotClientTransport(SSLContext sslContext, Map<String, String> headers, String scheme) {
    _headers = headers;
    _scheme = scheme;
    _sslContext = sslContext;

    AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
    if (_sslContext != null) {
      builder.setSSLContext(_sslContext);
    }

    _httpClient = new AsyncHttpClient(builder.build());
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException {
    try {
      return executeQueryAsync(brokerAddress, query).get();
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, final String query) {
    return executeQueryAsync(brokerAddress, new Request("pql", query));
  }

  public Future<BrokerResponse> executePinotQueryAsync(String brokerAddress, final Request request) {
    try {
      ObjectNode json = JsonNodeFactory.instance.objectNode();
      String queryFormat = request.getQueryFormat();
      json.put(queryFormat, request.getQuery());

      final String url;
      if (queryFormat.equalsIgnoreCase("sql")) {
        url = _scheme + "://" + brokerAddress + "/query/sql";
        json.put("queryOptions", "groupByMode=sql;responseFormat=sql");
      } else {
        url = _scheme + "://" + brokerAddress + "/query";
      }

      AsyncHttpClient.BoundRequestBuilder requestBuilder = _httpClient.preparePost(url);

      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      final Future<Response> response =
          requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").setBody(json.toString())
              .execute();

      return new BrokerResponseFuture(response, request.getQuery(), url);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, Request request)
      throws PinotClientException {
    try {
      return executeQueryAsync(brokerAddress, request).get();
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
      throws PinotClientException {
    return executePinotQueryAsync(brokerAddress, request);
  }

  @Override
  public void close()
      throws PinotClientException {
    if (_httpClient.isClosed()) {
      throw new PinotClientException("Connection is already closed!");
    }
    _httpClient.close();
  }

  private static class BrokerResponseFuture implements Future<BrokerResponse> {
    private final Future<Response> _response;
    private final String _query;
    private final String _url;

    public BrokerResponseFuture(Future<Response> response, String query, String url) {
      _response = response;
      _query = query;
      _url = url;
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
      return get(1000L, TimeUnit.DAYS);
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

        String responseBody = httpResponse.getResponseBody("UTF-8");
        return BrokerResponse.fromJson(OBJECT_READER.readTree(responseBody));
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }
  }
}
