package com.linkedin.pinot.controller.api.reslet.resources;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;



/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 *
 * sample curl call : curl -F campaignInsights_adsAnalysis-bmCamp_11=@campaignInsights_adsAnalysis-bmCamp_11      http://localhost:8998/segments
 *
 */
public class PinotFileUpload extends ServerResource {
  private static final Logger logger = Logger.getLogger(PinotFileUpload.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;

  public PinotFileUpload() {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager = (PinotHelixResourceManager) getApplication().getContext().getAttributes().get(PinotHelixResourceManager.class.toString());
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      // final String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = new StringRepresentation("this is a string");
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    Representation rep = null;
    System.out.println(conf.toString());
    try {
      final String fileName = "/tmp/";

      // 1/ Create a factory for disk-based file items
      final DiskFileItemFactory factory = new DiskFileItemFactory();


      // 2/ Create a new file upload handler based on the Restlet
      // FileUpload extension that will parse Restlet requests and
      // generates FileItems.
      final RestletFileUpload upload = new RestletFileUpload(factory);
      final List<FileItem> items;

      // 3/ Request is parsed by the handler which generates a
      // list of FileItems
      items = upload.parseRequest(getRequest());

      boolean found = false;
      for (final Iterator<FileItem> it = items.iterator(); it
          .hasNext()
          && !found;) {
        final FileItem fi = it.next();
        if (fi.getFieldName() != null) {
          found = true;
          final File file = new File(fileName + fi.getFieldName());
          fi.write(file);
        }
      }

      // Once handled, the content of the uploaded file is sent
      // back to the client.
      if (found) {
        // Create a new representation based on disk file.
        // The content is arbitrarily sent as plain text.
        rep = new StringRepresentation(fileName + " sucessfully uploaded",
            MediaType.TEXT_PLAIN);
      } else {
        // Some problem occurs, sent back a simple line of text.
        rep = new StringRepresentation("no file uploaded",
            MediaType.TEXT_PLAIN);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      logger.error(e);
    }
    return rep;
  }

}
