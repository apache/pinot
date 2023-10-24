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
package org.apache.pinot.client.controller;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.ConnectionTimeouts;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.TlsProtocols;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;
import org.apache.pinot.client.controller.response.SchemaResponse;
import org.apache.pinot.client.controller.response.TableResponse;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotControllerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerTransport.class);

  Map<String, String> _headers;
  private final String _scheme;
  private final AsyncHttpClient _httpClient;

  public PinotControllerTransport(Map<String, String> headers, String scheme, @Nullable SSLContext sslContext,
      ConnectionTimeouts connectionTimeouts, TlsProtocols tlsProtocols, @Nullable String appId) {
    _headers = headers;
    _scheme = scheme;

    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.OPTIONAL));
    }

    builder.setReadTimeout(connectionTimeouts.getReadTimeoutMs())
        .setConnectTimeout(connectionTimeouts.getConnectTimeoutMs())
        .setHandshakeTimeout(connectionTimeouts.getHandshakeTimeoutMs())
        .setUserAgent(getUserAgentVersionFromClassPath(appId))
        .setEnabledProtocols(tlsProtocols.getEnabledProtocols().toArray(new String[0]));

    _httpClient = Dsl.asyncHttpClient(builder.build());
  }

  private String getUserAgentVersionFromClassPath(@Nullable String appId) {
    Properties userAgentProperties = new Properties();
    try {
      userAgentProperties.load(
          PinotControllerTransport.class.getClassLoader().getResourceAsStream("version.properties"));
    } catch (IOException e) {
      LOGGER.warn("Unable to set user agent version", e);
    }
    String userAgentFromProperties = userAgentProperties.getProperty("ua", "unknown");
    if (StringUtils.isNotEmpty(appId)) {
      return
          appId.substring(0, Math.min(org.apache.pinot.client.utils.ConnectionUtils.APP_ID_MAX_CHARS, appId.length()))
              + "-" + userAgentFromProperties;
    }
    return userAgentFromProperties;
  }

  public TableResponse getAllTables(String controllerAddress) {
    try {
      String url = _scheme + "://" + controllerAddress + "/tables";
      BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      final Future<Response> response =
          requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").execute();

      TableResponse.TableResponseFuture tableResponseFuture = new TableResponse.TableResponseFuture(response, url);
      return tableResponseFuture.get();
    } catch (ExecutionException e) {
      throw new PinotClientException(e);
    }
  }

  public SchemaResponse getTableSchema(String table, String controllerAddress) {
    try {
      String url = _scheme + "://" + controllerAddress + "/tables/" + table + "/schema";
      BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      final Future<Response> response =
          requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").execute();

      SchemaResponse.SchemaResponseFuture schemaResponseFuture = new SchemaResponse.SchemaResponseFuture(response, url);
      return schemaResponseFuture.get();
    } catch (ExecutionException e) {
      throw new PinotClientException(e);
    }
  }

  public ControllerTenantBrokerResponse getBrokersFromController(String controllerAddress, String tenant) {
    try {
      String url = _scheme + "://" + controllerAddress + "/v2/brokers/tenants/" + tenant;
      BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      final Future<Response> response =
          requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").execute();

      ControllerTenantBrokerResponse.ControllerTenantBrokerResponseFuture controllerTableBrokerResponseFuture =
          new ControllerTenantBrokerResponse.ControllerTenantBrokerResponseFuture(response, url);
      return controllerTableBrokerResponseFuture.get();
    } catch (ExecutionException e) {
      throw new PinotClientException(e);
    }
  }

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
}
