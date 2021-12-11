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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO Remove this class and find out how to use LoggingFeature as described in
// https://jersey.github.io/documentation/latest/logging_chapter.html#d0e15719
@Provider
public class ControllerResponseFilter implements ContainerResponseFilter {
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerResponseFilter.class);

  @Inject
  private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> _request;

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    final String method = requestContext.getMethod();
    final String uri = requestContext.getUriInfo().getRequestUri().toString();
    final int respStatus = responseContext.getStatus();
    final String reasonPhrase = responseContext.getStatusInfo().getReasonPhrase();
    final String srcIpAddr = _request.get().getRemoteAddr();
    final String contentType = requestContext.getHeaderString(HttpHeaders.CONTENT_TYPE);
    LOGGER.info("Handled request from {} {} {}, content-type {} status code {} {}", srcIpAddr, method, uri, contentType,
        respStatus, reasonPhrase);
  }
}
