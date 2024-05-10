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
package org.apache.pinot.controller;

import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;


/**
 * Extends {@link CLStaticHttpHandler} to prevent file/directory listing.
 */
public class PinotCLStaticHttpHandler extends CLStaticHttpHandler {

  public PinotCLStaticHttpHandler(ClassLoader classLoader, String... docRoots) {
    super(classLoader, docRoots);
  }

  @Override
  protected boolean handle(String resourcePath, final Request request, final Response response)
      throws Exception {
    if (resourcePath.startsWith("/file:")) {
      // Set resource to forbidden to prevent directory listing
      response.setStatus(403);
      response.getWriter().write("Forbidden");
      return false;
    }
    return super.handle(resourcePath, request, response);
  }
}
