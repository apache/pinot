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
package org.apache.pinot.server.api;

import java.util.List;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.HttpRequesterIdentity;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.StaticHttpHandlerBase;
import org.glassfish.grizzly.http.util.HttpStatus;


/// Applies Server administrative authorization to a directly registered Grizzly HTTP handler.
///
/// The wrapped handler's lifecycle is delegated. File caching is disabled for static handlers because Grizzly serves
/// cache hits before invoking this wrapper's authorization check. The wrapper stores no mutable request state and is
/// safe for concurrent use when the delegate and access-control factory are safe for concurrent use.
public class ServerAdminApiAccessControlHandler extends HttpHandler {
  private static final String UNAUTHORIZED_MESSAGE = "Unauthorized";
  private static final String FORBIDDEN_MESSAGE = "Forbidden";

  private final HttpHandler _delegate;
  private final AccessControlFactory _accessControlFactory;

  public ServerAdminApiAccessControlHandler(HttpHandler delegate, AccessControlFactory accessControlFactory) {
    super(delegate.getName());
    _delegate = delegate;
    _accessControlFactory = accessControlFactory;
    if (delegate instanceof StaticHttpHandlerBase) {
      // Grizzly serves file-cache hits before HttpHandler.service(), which would bypass this authorization wrapper.
      ((StaticHttpHandlerBase) delegate).setFileCacheEnabled(false);
    }
  }

  @Override
  public void service(Request request, Response response)
      throws Exception {
    AuthorizationResult authorizationResult;
    try {
      authorizationResult =
          _accessControlFactory.create().authorizeAdminAccess(new HttpRequesterIdentity(request));
    } catch (NotAuthorizedException e) {
      List<Object> challenges = e.getChallenges();
      if (challenges != null) {
        for (Object challenge : challenges) {
          response.addHeader(HttpHeaders.WWW_AUTHENTICATE, challenge.toString());
        }
      }
      response.sendError(HttpStatus.UNAUTHORIZED_401.getStatusCode(), UNAUTHORIZED_MESSAGE);
      return;
    }
    if (!authorizationResult.hasAccess()) {
      response.sendError(HttpStatus.FORBIDDEN_403.getStatusCode(), FORBIDDEN_MESSAGE);
      return;
    }
    _delegate.service(request, response);
  }

  @Override
  public void start() {
    _delegate.start();
  }

  @Override
  public void destroy() {
    _delegate.destroy();
  }
}
