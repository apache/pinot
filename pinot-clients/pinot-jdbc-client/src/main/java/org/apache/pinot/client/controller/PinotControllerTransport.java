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

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;
import org.apache.pinot.client.controller.response.SchemaResponse;
import org.apache.pinot.client.controller.response.TableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotControllerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerTransport.class);

  AsyncHttpClient _httpClient = new AsyncHttpClient();
  Map<String, String> _headers;

  public PinotControllerTransport() {
  }

  public PinotControllerTransport(Map<String, String> headers) {
    _headers = headers;
  }

  public TableResponse getAllTables(String controllerAddress) {
    try {
      String url = "http://" + controllerAddress + "/tables";
      AsyncHttpClient.BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
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
      String url = "http://" + controllerAddress + "/tables/" + table + "/schema";
      AsyncHttpClient.BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
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
      String url = "http://" + controllerAddress + "/brokers/tenants/" + tenant;
      AsyncHttpClient.BoundRequestBuilder requestBuilder = _httpClient.prepareGet(url);
      if (_headers != null) {
        _headers.forEach((k, v) -> requestBuilder.addHeader(k, v));
      }

      final Future<Response> response =
              requestBuilder.addHeader("Content-Type", "application/json; charset=utf-8").execute();

      ControllerTenantBrokerResponse.ControllerTenantBrokerResponseFuture controllerTableBrokerResponseFuture = new ControllerTenantBrokerResponse.ControllerTenantBrokerResponseFuture(response, url);
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
    _httpClient.close();
  }

}
