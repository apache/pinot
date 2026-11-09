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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.spi.utils.Obfuscator;
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
    final String uri = redactQueryParameters(requestContext.getUriInfo().getRequestUri());
    final int respStatus = responseContext.getStatus();
    final String reasonPhrase = responseContext.getStatusInfo().getReasonPhrase();
    final String srcIpAddr = _request.get().getRemoteAddr();
    final String contentType = requestContext.getHeaderString(HttpHeaders.CONTENT_TYPE);
    LOGGER.info("Handled request from {} {} {}, content-type {} status code {} {}", srcIpAddr, method, uri, contentType,
        respStatus, reasonPhrase);
  }

  /// Returns the request URI with the values of credential-bearing query parameters masked, as decided by
  /// [Obfuscator#DEFAULT]. This line is logged at INFO for every request, so a token passed as a query
  /// parameter would otherwise be written to the controller log verbatim. Other parameters are kept in
  /// their original order with their values as sent, since they are what makes the line useful when
  /// debugging. Names are shown decoded, and repeated parameters are grouped under their first occurrence.
  @VisibleForTesting
  static String redactQueryParameters(URI requestUri) {
    String rawQuery = requestUri.getRawQuery();
    String uri = requestUri.toString();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return uri;
    }
    String withoutQuery = uri.substring(0, uri.indexOf('?'));
    // Keyed by the decoded name, which is what the patterns judge; the values stay raw. A URI has already
    // rejected malformed escapes, so decoding cannot fail here.
    Map<String, List<String>> parameters = new LinkedHashMap<>();
    for (String pair : rawQuery.split("&")) {
      int separator = pair.indexOf('=');
      String rawName = separator < 0 ? pair : pair.substring(0, separator);
      String rawValue = separator < 0 ? "" : pair.substring(separator + 1);
      parameters.computeIfAbsent(URLDecoder.decode(rawName, StandardCharsets.UTF_8), k -> new ArrayList<>())
          .add(rawValue);
    }
    JsonNode redacted = Obfuscator.DEFAULT.toJson(parameters);
    StringJoiner query = new StringJoiner("&", "?", "");
    redacted.fields().forEachRemaining(parameter -> {
      String name = parameter.getKey();
      JsonNode value = parameter.getValue();
      if (value.isArray()) {
        value.forEach(element -> query.add(name + "=" + element.asText()));
      } else {
        // A masked parameter is replaced as a whole, so the number of values it had is not disclosed.
        query.add(name + "=" + value.asText());
      }
    });
    return withoutQuery + query;
  }
}
