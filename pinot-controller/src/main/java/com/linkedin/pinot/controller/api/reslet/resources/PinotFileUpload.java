package com.linkedin.pinot.controller.api.reslet.resources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


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
  private final File baseDataDir;
  private final File tempDir;
  private final File tempUntarredPath;
  private final String vip;

  public PinotFileUpload() throws IOException {

    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    baseDataDir = new File(conf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "temp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
    tempUntarredPath = new File(tempDir, "untarred");
    if (!tempUntarredPath.exists()) {
      tempUntarredPath.mkdirs();
    }
    vip = StringUtil.join("://", "http", StringUtil.join(":", conf.getControllerVipHost(), conf.getControllerPort()));
    logger.info("controller download url base is : " + vip);
  }

  @Override
  public Representation get() {
    Representation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");

      if ((resourceName == null) && (segmentName == null)) {
        final JSONArray ret = new JSONArray();
        for (final File file : baseDataDir.listFiles()) {
          final String url =
              "http://" + StringUtil.join(":", conf.getControllerVipHost(), conf.getControllerPort()) + "/datafiles/"
                  + file.getName();
          ret.put(url);
        }
        presentation = new StringRepresentation(ret.toString());
        return presentation;

      } else if ((resourceName != null) && (segmentName == null)) {
        final JSONArray ret = new JSONArray();
        for (final File file : new File(baseDataDir, resourceName).listFiles()) {
          final String url =
              "http://" + StringUtil.join(":", conf.getControllerVipHost(), conf.getControllerPort()) + "/datafiles/"
                  + resourceName + "/" + file.getName();
          ret.put(url);
        }
        presentation = new StringRepresentation(ret.toString());
        return presentation;
      }

      final File dataFile = new File(baseDataDir, StringUtil.join("/", resourceName, segmentName));
      if (dataFile.exists()) {
        presentation = new FileRepresentation(dataFile, MediaType.ALL, 0);
        return presentation;
      }
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
    File tmpSegmentDir = null;
    File dataFile = null;
    try {

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
      for (final Iterator<FileItem> it = items.iterator(); it.hasNext() && !found;) {
        final FileItem fi = it.next();
        if (fi.getFieldName() != null) {
          found = true;
          dataFile = new File(tempDir, fi.getFieldName());
          fi.write(dataFile);
        }
      }

      // Once handled, the content of the uploaded file is sent
      // back to the client.
      if (found) {
        // Create a new representation based on disk file.
        // The content is arbitrarily sent as plain text.
        rep = new StringRepresentation(dataFile + " sucessfully uploaded", MediaType.TEXT_PLAIN);
        if (tempUntarredPath.exists()) {
          FileUtils.deleteDirectory(tempUntarredPath);
        }
        tmpSegmentDir = new File(tempUntarredPath, dataFile.getName());
        logger.info("Untar segment to temp dir: " + tmpSegmentDir);
        if (!tmpSegmentDir.exists()) {
          tmpSegmentDir.mkdirs();
        }
        TarGzCompressionUtils.unTar(dataFile, tmpSegmentDir);

        final SegmentMetadata metadata = new SegmentMetadataImpl(tmpSegmentDir.listFiles()[0]);
        final File resourceDir = new File(baseDataDir, metadata.getResourceName());
        File segmentFile = new File(resourceDir, dataFile.getName());
        if (segmentFile.exists()) {
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(dataFile, segmentFile);
        PinotResourceManagerResponse res =
            manager.addSegment(metadata, constructDownloadUrl(metadata.getResourceName(), dataFile.getName()));
        if (!res.isSuccessfull()) {
          rep = new StringRepresentation(res.errorMessage, MediaType.TEXT_PLAIN);
          setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY);
          FileUtils.deleteQuietly(new File(resourceDir, dataFile.getName()));
        }
      } else {
        // Some problem occurs, sent back a simple line of text.
        rep = new StringRepresentation("no file uploaded", MediaType.TEXT_PLAIN);
        setStatus(Status.SERVER_ERROR_INTERNAL);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      logger.error(e);
      setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY);
    } finally {
      if ((tmpSegmentDir != null) && tmpSegmentDir.exists()) {
        try {
          FileUtils.deleteDirectory(tmpSegmentDir);
        } catch (final IOException e) {
          e.printStackTrace();
          logger.error(e);
        }
      }
      if ((dataFile != null) && dataFile.exists()) {
        FileUtils.deleteQuietly(dataFile);
      }
    }
    return rep;
  }

  @Override
  @Delete
  public Representation delete() {
    Representation rep = null;
    final String resourceName = (String) getRequest().getAttributes().get("resourceName");
    final String segmentName = (String) getRequest().getAttributes().get("segmentName");
    logger.info("Getting segment deletion request, resourceName: " + resourceName + " segmentName: " + segmentName);
    if (resourceName == null || segmentName == null) {
      throw new RuntimeException("either resource name or segment name is null");
    }
    final PinotResourceManagerResponse res = manager.deleteSegment(resourceName, segmentName);
    rep = new StringRepresentation(res.toString());
    return rep;
  }

  public String constructDownloadUrl(String resouceName, String segmentName) {
    final String ret = StringUtil.join("/", vip, "datafiles", resouceName, segmentName);
    return ret;
  }
}
