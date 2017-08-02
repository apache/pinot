/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;


// A class to add the headers that we expect from clients. Pre-jersey API, the clients have been lax in specifying the
// actualy content type that they POST or PUT. Most cases, it is JSON. In the jersey API, we have the power to receive
// objects as a typed object, if it can be de-serialized into one. So, we resort to replacing the content type with
// the expected one.
@PreMatching
@Provider
public class HeaderAdder implements ContainerRequestFilter {

  // Method is PUT or POST, we need to decide if a header is to be inserted for body being JSON
  private String modifiedContentType(String path) {
    if (path.startsWith("tenants")) {
      return MediaType.APPLICATION_JSON;
    }
    if (path.startsWith("instances")) {
      // If we post a new instance, that is in JSON
      // But if we enable/disable the state of an instance, that is plain text.
      String[] pathParts = path.split("/");
      if (pathParts[pathParts.length-1].equals("state")) {
        return MediaType.TEXT_PLAIN;
      }
      return MediaType.APPLICATION_JSON;
    }
    return null;
  }

  @Override
  public void filter(ContainerRequestContext req) throws IOException {
    String path = req.getUriInfo().getPath();
    if ((req.getMethod().equalsIgnoreCase("PUT") ||
        req.getMethod().equalsIgnoreCase("POST"))) {
      String mediaType = modifiedContentType(path);
      if (mediaType != null) {
        req.getHeaders().remove(HttpHeaders.CONTENT_TYPE);
        req.getHeaders().add(HttpHeaders.CONTENT_TYPE, mediaType);
      }
    }
  }
}

