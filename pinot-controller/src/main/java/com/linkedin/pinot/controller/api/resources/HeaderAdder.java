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


@PreMatching
@Provider
public class HeaderAdder implements ContainerRequestFilter {

  private boolean shouldBeJsonInput(String path) {
    if (
        path.startsWith("instances") ||
//            path.startsWith("schemas") ||
            path.startsWith("tenants")
        ) {
      return true;
    }
    return false;

  }

  @Override
  public void filter(ContainerRequestContext req) throws IOException {
    String path = req.getUriInfo().getPath();
    if ((req.getMethod().equalsIgnoreCase("PUT") ||
        req.getMethod().equalsIgnoreCase("POST") ) &&
        shouldBeJsonInput(path)) {
      req.getHeaders().remove(HttpHeaders.CONTENT_TYPE);
      req.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    }
  }
}

