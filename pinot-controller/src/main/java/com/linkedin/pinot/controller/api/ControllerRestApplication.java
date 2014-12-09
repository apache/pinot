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
import com.linkedin.pinot.controller.api.reslet.resources.PinotDataResource;
import com.linkedin.pinot.controller.api.reslet.resources.PinotFileUpload;
import com.linkedin.pinot.controller.api.reslet.resources.PinotInstance;
import com.linkedin.pinot.controller.api.reslet.resources.PinotSegment;
import com.linkedin.pinot.controller.api.reslet.resources.RunPql;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
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

    router.attach("/dataresources", PinotDataResource.class);
    router.attach("/dataresources/", PinotDataResource.class);
    router.attach("/dataresources/{resourceName}", PinotDataResource.class);

    router.attach("/dataresources/{resourceName}/segments", PinotSegment.class);
    router.attach("/dataresources/{resourceName}/segments/{segmentName}", PinotSegment.class);

    router.attach("/instances", PinotInstance.class);
    router.attach("/instances/", PinotInstance.class);

    router.attach("/datafiles", PinotFileUpload.class);
    router.attach("/datafiles/{resourceName}", PinotFileUpload.class);
    router.attach("/datafiles/{resourceName}/{segmentName}", PinotFileUpload.class);

    router.attach("/pinot-controller/admin", PinotControllerHealthCheck.class);

    router.attach("/pql", RunPql.class);

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
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/export/content/data/pinot/dataDir/xlntBeta/xlntBeta_product_email_2"),
            ReadMode.heap);
    while (true) {

    }
  }
}
