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

import java.io.File;

import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Directory;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.controller.api.reslet.resources.PinotControllerHealthCheck;
import com.linkedin.pinot.controller.api.reslet.resources.PinotInstance;
import com.linkedin.pinot.controller.api.reslet.resources.PqlQueryResource;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotSchemaResletResource;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotSegmentResletResource;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotSegmentUploadRestletResource;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableIndexingConfigs;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableInstances;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableMetadataConfigs;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableRestletResource;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableSegmentConfigs;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTableTenantConfigs;
import com.linkedin.pinot.controller.api.reslet.resources.v2.PinotTenantRestletResource;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


/**
 * Sep 24, 2014
 */

public class ControllerRestApplication extends Application {

  private static String CONSOLE_WEBAPP_ROOT_PATH;

  public ControllerRestApplication(String queryConsolePath) {
    super();
    CONSOLE_WEBAPP_ROOT_PATH = queryConsolePath;
  }

  public ControllerRestApplication(Context context) {
    super(context);
  }

  @Override
  public Restlet createInboundRoot() {
    final Router router = new Router(getContext());
    router.setDefaultMatchingMode(Template.MODE_EQUALS);

    /**
     * Start Routers 2.0
     */

    router.attach("/tenants", PinotTenantRestletResource.class);
    router.attach("/tenants/", PinotTenantRestletResource.class);
    router.attach("/tenants/{tenantName}", PinotTenantRestletResource.class);
    router.attach("/tenants/{tenantName}/", PinotTenantRestletResource.class);
    router.attach("/tenants/{tenantName}/instances", PinotTenantRestletResource.class);

    router.attach("/schemas", PinotSchemaResletResource.class);
    router.attach("/schemas/", PinotSchemaResletResource.class);
    router.attach("/schemas/{schemaName}", PinotSchemaResletResource.class);

    // POST
    router.attach("/tables", PinotTableRestletResource.class);

    // GET
    router.attach("/tables/{tableName}", PinotTableRestletResource.class);
    router.attach("/tables/{tableName}/instances", PinotTableInstances.class);

    router.attach("/tables/{tableName}/segments", PinotSegmentResletResource.class);
    router.attach("/tables/{tableName}/segments/{segmentName}", PinotSegmentResletResource.class);

    // PUT
    router.attach("/tables/{tableName}/segmentsConfigs", PinotTableSegmentConfigs.class);
    router.attach("/tables/{tableName}/indexingConfigs", PinotTableIndexingConfigs.class);
    router.attach("/tables/{tableName}/tenantConfigs", PinotTableTenantConfigs.class);
    router.attach("/tables/{tableName}/metadataConfigs", PinotTableMetadataConfigs.class);

    // Uploading Downloading segments
    router.attach("/segments", PinotSegmentUploadRestletResource.class);
    router.attach("/segments/{tableName}", PinotSegmentUploadRestletResource.class);
    router.attach("/segments/{tableName}/{segmentName}", PinotSegmentUploadRestletResource.class);

    /**
     *  End Routes 2.0
     */

    router.attach("/instances", PinotInstance.class);
    router.attach("/instances/", PinotInstance.class);

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
    router.attach("/query", webdir);

    return router;
  }

  public static void main(String[] args) throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File(
            "/export/content/data/pinot/dataDir/xlntBeta/xlntBeta_product_email_2"), ReadMode.heap);
    while (true) {

    }
  }
}
