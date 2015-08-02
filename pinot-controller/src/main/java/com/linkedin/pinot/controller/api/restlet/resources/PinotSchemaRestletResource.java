package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;


public class PinotSchemaRestletResource extends PinotRestletResourceBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaRestletResource.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotSchemaRestletResource() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    try {
      final String schemaName = (String) getRequest().getAttributes().get("schemaName");
      if (schemaName != null) {
        return getSchema(schemaName);
      } else {
        return getAllSchemas();
      }
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get a list of all schemas")
  @Tags({"schema"})
  @Paths({
      "/schemas",
      "/schemas/"
  })
  private Representation getAllSchemas() {
    List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames();
    JSONArray ret = new JSONArray();

    if (schemaNames == null){
      return new StringRepresentation(ret.toString());
    }

    for (String schema : schemaNames) {
      ret.put(schema);
    }
    return new StringRepresentation(ret.toString());
  }

  @HttpVerb("get")
  @Summary("Gets a schema")
  @Tags({"schema"})
  @Paths({
      "/schemas/{schemaName}",
      "/schemas/{schemaName}/"
  })
  private Representation getSchema(
      @Parameter(name = "schemaName", in = "path", description = "The name of the schema to get")
      String schemaName)
      throws IOException {
    LOGGER.info("looking for schema {}", schemaName);
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      return new StringRepresentation("{}");
    }
    LOGGER.info("schema string is : " + schema.getJSONSchema());
    return new StringRepresentation(schema.getJSONSchema());
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    Representation rep = null;
    File dataFile = null;
    try {
      rep = uploadSchema(dataFile);
    } catch (final Exception e) {
      rep = PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception in file upload", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return rep;
  }

  @HttpVerb("post")
  @Summary("Adds a schema")
  @Tags({"schema"})
  @Paths({
      "/schemas",
      "/schemas/"
  })
  private Representation uploadSchema(File dataFile)
      throws Exception {
    Representation rep;// 1/ Create a factory for disk-based file items
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
    for (final Iterator<FileItem> it = items.iterator(); it.hasNext() && !found;) {
      final FileItem fi = it.next();
      if (fi.getFieldName() != null) {
        found = true;
        dataFile = new File(tempDir, fi.getFieldName() + "-" + System.currentTimeMillis());
        fi.write(dataFile);
      }
    }

    // Once handled, the content of the uploaded file is sent
    // back to the client.
    if (found) {
      // Create a new representation based on disk file.
      // The content is arbitrarily sent as plain text.
      Schema schema = Schema.fromFile(dataFile);
      try {
        _pinotHelixResourceManager.addSchema(schema);
        rep = new StringRepresentation(dataFile + " sucessfully added", MediaType.TEXT_PLAIN);
      } catch (Exception e) {
        LOGGER.error("error adding schema ", e);
        rep = PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
        LOGGER.error("Caught exception in file upload", e);
        setStatus(Status.SERVER_ERROR_INTERNAL);
      }
    } else {
      // Some problem occurs, sent back a simple line of text.
      rep = new StringRepresentation("schema not added", MediaType.TEXT_PLAIN);
      LOGGER.warn("No file was uploaded");
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return rep;
  }
}
