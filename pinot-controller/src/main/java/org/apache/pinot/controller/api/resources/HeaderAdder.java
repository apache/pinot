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
import java.net.URI;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// A class to add the headers that we expect from clients. Pre-jersey API, the clients have been lax in specifying the
// actual content type that they POST or PUT. Most cases, it is JSON. In the jersey API, we have the power to receive
// objects as a typed object, if it can be de-serialized into one. So, we resort to replacing the content type with
// the expected one.
@PreMatching
@Provider
@Priority(1)
public class HeaderAdder implements ContainerRequestFilter {
  public static final Logger LOGGER = LoggerFactory.getLogger(HeaderAdder.class);

  // Method is PUT or POST, we need to decide if a header is to be inserted for body being JSON
  private String modifiedContentType(String path) {
    if (path.startsWith("tenants")) {
      return MediaType.APPLICATION_JSON;
    }
    if (path.startsWith("instances")) {
      // If we post a new instance, that is in JSON
      // But if we enable/disable the state of an instance, that is plain text.
      String[] pathParts = path.split("/");
      if (pathParts[pathParts.length - 1].equals("state")) {
        return MediaType.TEXT_PLAIN;
      }
      return MediaType.APPLICATION_JSON;
    }
    return null;
  }

  @Override
  public void filter(ContainerRequestContext req)
      throws IOException {
    {
      // TODO HACK TO BE REMOVED ONCE CLIENTS HAVE UPGRADED TO NEW THIRD-EYE JAR
      // When a client sends an HTTP request without the leading slash (e.g. "GET tables HTTP/1.1")
      // then jersey seems to not parse it correctly. In these cases, the "incomingBaseUri" turns out as
      // http://localhost:21000/ and the "incomingReqUri" turns out as "http://localhost:21000tables"
      // We want to rewrite the incoming request URI to include the slash, otherwise, "localhost:21000tables" is parsed
      // as the authority.
      //
      // An older version of the http client used by third-eye jar sent http requests without leading slash
      // in the URI, causing jersey APIs to break.
      final String incomingReqUri = req.getUriInfo().getRequestUri().toString();
      final String incomingBaseUri = req.getUriInfo().getBaseUri().toASCIIString();
      try {
        String baseUriWithoutSlash = incomingBaseUri;
        if (incomingBaseUri.endsWith("/")) {
          // Remove the trailing slash
          baseUriWithoutSlash = incomingBaseUri.substring(0, incomingBaseUri.length() - 1);
        }
        if (incomingReqUri.startsWith(baseUriWithoutSlash)) {
          String relativeUri = incomingReqUri.substring(baseUriWithoutSlash.length());
          // In the example described above, relativeUri will be "tables"
          if (!relativeUri.startsWith("/")) {
            URI newReqUri = new URI(baseUriWithoutSlash + "/" + relativeUri);
            LOGGER
                .warn("Rewriting new Request URI {} (incomingBaseUri = {}, incomingReqUri = {})", newReqUri.toString(),
                    incomingBaseUri, incomingReqUri);
            req.setRequestUri(newReqUri);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Exception handling incoming URI {}, base URI {}", incomingReqUri, incomingBaseUri);
      }
    }

    // Add outgoing headers if this is a PUT or a POST
    String path = req.getUriInfo().getPath();
    if ((req.getMethod().equalsIgnoreCase("PUT") || req.getMethod().equalsIgnoreCase("POST"))) {
      String mediaType = modifiedContentType(path);
      if (mediaType != null) {
        req.getHeaders().remove(HttpHeaders.CONTENT_TYPE);
        req.getHeaders().add(HttpHeaders.CONTENT_TYPE, mediaType);
      }
    }
  }
}
