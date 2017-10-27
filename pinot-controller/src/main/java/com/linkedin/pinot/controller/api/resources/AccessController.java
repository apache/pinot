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

import com.linkedin.pinot.controller.api.access.AccessControl;
import com.linkedin.pinot.controller.api.access.AccessControlFactory;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
public class AccessController implements ContainerRequestFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccessController.class);

  @Inject
  AccessControlFactory _accessControlFactory;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.isAllowed(requestContext)) {
      throw new ControllerApplicationException(LOGGER,
          "Access denied for URI: " + requestContext.getUriInfo().getRequestUri(), Response.Status.FORBIDDEN);
    }
  }
}
