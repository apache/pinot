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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;
import org.apache.pinot.client.controller.response.SchemaResponse;
import org.apache.pinot.client.controller.response.TableResponse;
import org.apache.pinot.spi.utils.CommonConstants;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotControllerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerTransport.class);

  private static final int READ_TIMEOUT_MS = 60000;

  private static final int CONNECT_TIMEOUT_MS = 2000;

  private static final int HANDSHAKE_TIMEOUT_MS = 2000;

  Map<String, String> _headers;
  private final String _scheme;
  private final AsyncHttpClient _httpClient;


  public PinotControllerTransport() {
    this(Collections.emptyMap(), CommonConstants.HTTP_PROTOCOL, null,
            READ_TIMEOUT_MS, CONNECT_TIMEOUT_MS, HANDSHAKE_TIMEOUT_MS, false);
  }

  public PinotControllerTransport(Map<String, String> headers, String scheme,
                                  @Nullable SSLContext sslContext, int readTimeout,
                                  int connectTimeout, int handshakeTimeout, boolean tlsV10Enabled) {
    _headers = headers;
    _scheme = scheme;

    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.OPTIONAL));
    }

    Properties prop = new Properties();
    try {
      prop.load(PinotControllerTransport.class.getClassLoader().getResourceAsStream("version.properties"));
    } catch (IOException e) {
      LOGGER.info("Unable to set user agent version");
    }

    builder.setReadTimeout(readTimeout)
            .setConnectTimeout(connectTimeout)
            .setHandshakeTimeout(handshakeTimeout)
            .setUserAgent(prop.getProperty("ua", "pinot-jdbc"))
            .setEnabledProtocols(createEnabledProtocols(tlsV10Enabled));

    _httpClient = Dsl.asyncHttpClient(builder.build());
  }

  private String[] createEnabledProtocols(boolean tlsV10Enabled) {
    List<String> enabledProtocols = new ArrayList<>();
    enabledProtocols.add("TLSv1.3");
    enabledProtocols.add("TLSv1.2");
    enabledProtocols.add("TLSv1.1");
    if (tlsV10Enabled) {
      enabledProtocols.add("TLSv1.0");
    }
    LOGGER.debug("Enabled TLS protocols: {}", enabledProtocols);
    return enabledProtocols.toArray(new String[0]);
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
