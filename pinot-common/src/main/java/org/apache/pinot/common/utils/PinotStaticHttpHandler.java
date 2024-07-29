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
package org.apache.pinot.common.utils;

import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;


/**
 * A secure static HTTP handler that prevents path traversal attacks.
 */
public class PinotStaticHttpHandler extends CLStaticHttpHandler {
  public PinotStaticHttpHandler(ClassLoader classLoader, String... docRoot) {
    super(classLoader, docRoot);
  }

  @Override
  public boolean handle(String resourcePath, Request request, Response response)
      throws Exception {
    if (isPathTraversal(resourcePath)) {
      response.setStatus(403); // Set HTTP status to 400 Bad Request
      response.getWriter().write("Forbidden");
      return false;
    } else {
      return super.handle(resourcePath, request, response); // Call the superclass method to handle normal serving
    }
  }

  /**
   * Check if the path contains path traversal patterns.
   * @param path The path to check
   * @return True if the path contains path traversal patterns, false otherwise
   */
  private boolean isPathTraversal(String path) {
    // Check for disallowed patterns and ensure the path doesn't contain suspicious protocols like 'file:///'
    return path.contains("file:") || path.contains("..");
  }
}
