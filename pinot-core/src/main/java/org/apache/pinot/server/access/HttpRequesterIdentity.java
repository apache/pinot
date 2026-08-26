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
package org.apache.pinot.server.access;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.glassfish.grizzly.http.server.Request;


/// Identity container for HTTP requests with (optional) authorization headers
public class HttpRequesterIdentity extends RequesterIdentity {
  private Multimap<String, String> _httpHeaders;
  private String _endpointUrl;

  public HttpRequesterIdentity(HttpHeaders httpHeaders) {
    _httpHeaders = HashMultimap.create();
    httpHeaders.getRequestHeaders().forEach(_httpHeaders::putAll);
  }

  public HttpRequesterIdentity(Request request) {
    _httpHeaders = HashMultimap.create();
    request.getHeaderNames().forEach(name -> request.getHeaders(name).forEach(value -> _httpHeaders.put(name, value)));
    String requestUrl = request.getRequestURL().toString();
    String queryString = request.getQueryString();
    _endpointUrl = queryString != null ? requestUrl + "?" + queryString : requestUrl;
  }

  /// Returns all values for a header name, using HTTP's case-insensitive name semantics.
  public Collection<String> getHeaderValues(String name) {
    List<String> values = new ArrayList<>();
    _httpHeaders.asMap().forEach((headerName, headerValues) -> {
      if (headerName.equalsIgnoreCase(name)) {
        values.addAll(headerValues);
      }
    });
    return values;
  }

  public Multimap<String, String> getHttpHeaders() {
    return _httpHeaders;
  }

  public void setHttpHeaders(Multimap<String, String> httpHeaders) {
    _httpHeaders = httpHeaders;
  }

  public String getEndpointUrl() {
    return _endpointUrl;
  }

  public void setEndpointUrl(String endpointUrl) {
    _endpointUrl = endpointUrl;
  }
}
