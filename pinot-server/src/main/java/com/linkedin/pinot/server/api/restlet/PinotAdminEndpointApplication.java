/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.api.restlet;

import com.linkedin.pinot.common.restlet.PinotRestletApplication;
import com.linkedin.pinot.common.restlet.swagger.SwaggerResource;
import org.restlet.resource.Directory;
import org.restlet.routing.Redirector;
import org.restlet.routing.Router;


/**
 * Restlet application for the Pinot server debug endpoint
 */
public class PinotAdminEndpointApplication extends PinotRestletApplication {
  @Override
  protected void configureRouter(Router router) {
    attachRoutesForClass(router, MmapDebugResource.class);

    // Attach Swagger stuff
    router.attach("/api", SwaggerResource.class);

    final Directory swaggerUiDir = new Directory(getContext(), getClass().getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.1.8-M1").toString());
    swaggerUiDir.setDeeplyAccessible(true);
    router.attach("/swagger-ui", swaggerUiDir);

    final Redirector redirector = new Redirector(getContext(), "/swagger-ui/index.html?url=/api", Redirector.MODE_CLIENT_TEMPORARY);
    router.attach("/help", redirector);
  }
}
