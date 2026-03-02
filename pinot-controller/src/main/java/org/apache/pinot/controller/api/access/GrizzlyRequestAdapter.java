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
package org.apache.pinot.controller.api.access;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Enumeration;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.glassfish.grizzly.http.server.Request;


public final class GrizzlyRequestAdapter extends HttpServletRequestWrapper {
  private final Request _grizzlyRequest;

  private GrizzlyRequestAdapter(Request grizzlyRequest) {
    super(createDummyRequest());
    _grizzlyRequest = grizzlyRequest;
  }

  @Nullable
  public static HttpServletRequest wrap(@Nullable Request request) {
    if (request == null) {
      return null;
    }
    return new GrizzlyRequestAdapter(request);
  }

  @Override
  public Object getAttribute(String name) {
    return _grizzlyRequest.getAttribute(name);
  }

  @Override
  public String getRemoteAddr() {
    return _grizzlyRequest.getRemoteAddr();
  }

  @Override
  public StringBuffer getRequestURL() {
    return new StringBuffer(_grizzlyRequest.getRequestURL().toString());
  }

  @Override
  public String getRequestURI() {
    return _grizzlyRequest.getRequestURI();
  }

  @Override
  public String getContextPath() {
    return _grizzlyRequest.getContextPath();
  }

  @Override
  public String getHeader(String name) {
    return _grizzlyRequest.getHeader(name);
  }

  @Override
  public Enumeration<String> getHeaders(String name) {
    String value = _grizzlyRequest.getHeader(name);
    if (value == null) {
      return Collections.emptyEnumeration();
    }
    return Collections.enumeration(Collections.singletonList(value));
  }

  private static HttpServletRequest createDummyRequest() {
    Object proxy = Proxy.newProxyInstance(
        HttpServletRequest.class.getClassLoader(),
        new Class<?>[]{HttpServletRequest.class},
        (p, method, args) -> {
          throw new UnsupportedOperationException(
              "Method " + method.getName() + " is not supported by GrizzlyRequestAdapter");
        });
    return HttpServletRequest.class.cast(proxy);
  }
}
