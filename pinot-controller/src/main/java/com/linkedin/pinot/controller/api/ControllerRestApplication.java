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
package com.linkedin.pinot.controller.api;

import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.restlet.PinotRestletApplication;
import com.linkedin.pinot.controller.api.restlet.resources.BasePinotControllerRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotVersionRestletResource;
import com.linkedin.pinot.common.restlet.swagger.SwaggerResource;

import com.linkedin.pinot.controller.api.restlet.resources.TableSize;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Directory;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Filter;
import org.restlet.routing.Redirector;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

import com.linkedin.pinot.controller.api.restlet.resources.PinotControllerHealthCheck;
import com.linkedin.pinot.controller.api.restlet.resources.PinotInstanceRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotSchemaRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotSegmentRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotSegmentUploadRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableIndexingConfigs;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableInstances;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableMetadataConfigs;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableSchema;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableSegmentConfigs;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTableTenantConfigs;
import com.linkedin.pinot.controller.api.restlet.resources.PinotTenantRestletResource;
import com.linkedin.pinot.controller.api.restlet.resources.PqlQueryResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerRestApplication extends PinotRestletApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRestApplication.class);
  private static final String ONE_HOUR_IN_MILLIS = Long.toString(3600L * 1000L);

  private static String CONSOLE_WEBAPP_ROOT_PATH;

  private static Router router;

  private static ControllerMetrics metrics;

  public ControllerRestApplication(String queryConsolePath) {
    super();
    CONSOLE_WEBAPP_ROOT_PATH = queryConsolePath;
  }

  public static void setControllerMetrics(ControllerMetrics controllerMetrics) {
    metrics = controllerMetrics;
  }

  public static ControllerMetrics getControllerMetrics() { return metrics; }

  protected void configureRouter(Router router) {
    // Remove server-side HTTP timeout (see http://stackoverflow.com/questions/12943447/restlet-server-socket-timeout)
    getContext().getParameters().add("maxIoIdleTimeMs", ONE_HOUR_IN_MILLIS);
    getContext().getParameters().add("ioMaxIdleTimeMs", ONE_HOUR_IN_MILLIS);

    router.setDefaultMatchingMode(Template.MODE_EQUALS);

    /**
     * Start Routers 2.0
     */

    attachRoutesForClass(router, PinotTenantRestletResource.class);
    attachRoutesForClass(router, PinotSchemaRestletResource.class);
    attachRoutesForClass(router, PinotTableRestletResource.class);

    // GET
    attachRoutesForClass(router, PinotTableInstances.class);
    attachRoutesForClass(router, PinotTableSchema.class);
    attachRoutesForClass(router, PinotSegmentRestletResource.class);
    attachRoutesForClass(router, TableSize.class);

    // PUT
    attachRoutesForClass(router, PinotTableSegmentConfigs.class);
    attachRoutesForClass(router, PinotTableIndexingConfigs.class);
    attachRoutesForClass(router, PinotTableTenantConfigs.class);
    attachRoutesForClass(router, PinotTableMetadataConfigs.class);

    // Uploading Downloading segments
    attachRoutesForClass(router, PinotSegmentUploadRestletResource.class);

    attachRoutesForClass(router, PinotVersionRestletResource.class);

    router.attach("/api", SwaggerResource.class);

    /**
     *  End Routes 2.0
     */

    attachRoutesForClass(router, PinotInstanceRestletResource.class);

    router.attach("/pinot-controller/admin", PinotControllerHealthCheck.class);

    router.attach("/pql", PqlQueryResource.class);

    final Restlet mainpage = new Restlet() {
      @Override
      public void handle(Request request, Response response) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<html>");
        stringBuilder.append("<head><title>Restlet Cluster Management page</title></head>");
        stringBuilder.append("<body bgcolor=white>");
        stringBuilder.append("<table border=\"0\">");
        stringBuilder.append("<tr>");
        stringBuilder.append("<td>");
        stringBuilder.append("<h1>Rest cluster management interface V1</h1>");
        stringBuilder.append("</td>");
        stringBuilder.append("</tr>");
        stringBuilder.append("</table>");
        stringBuilder.append("</body>");
        stringBuilder.append("</html>");
        response.setEntity(new StringRepresentation(stringBuilder.toString(), MediaType.TEXT_HTML));
      }
    };

    final Directory webdir = new Directory(getContext(), CONSOLE_WEBAPP_ROOT_PATH);
    webdir.setDeeplyAccessible(true);
    webdir.setIndexName("index.html");
    router.attach("/query", webdir);

    final Directory swaggerUiDir = new Directory(getContext(), getClass().getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.1.8-M1").toString());
    swaggerUiDir.setDeeplyAccessible(true);
    router.attach("/swagger-ui", swaggerUiDir);

    final Redirector redirector = new Redirector(getContext(), "/swagger-ui/index.html?url=/api", Redirector.MODE_CLIENT_TEMPORARY);
    router.attach("/help", redirector);
  }

  @Override
  protected void attachRoute(Router router, String routePath, Class<? extends ServerResource> clazz) {
    router.attach(routePath, new AddHeaderFilter(getContext(), createFinder(clazz)));
  }

  private class AddHeaderFilter extends Filter {
    public AddHeaderFilter(Context context, Restlet next) {
      super(context, next);
    }

    @Override
    protected int beforeHandle(Request request, Response response) {
      BasePinotControllerRestletResource.addExtraHeaders(response);
      return Filter.CONTINUE;
    }
  }

  public static Router getRouter() {
    return router;
  }

}
