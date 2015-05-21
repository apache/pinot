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
package com.linkedin.pinot.controller.api.reslet.resources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
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
@Deprecated
public class PinotFileUpload extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFileUpload.class);
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
    tempDir = new File(baseDataDir, "fileUploadTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
    tempUntarredPath = new File(tempDir, "untarred");
    if (!tempUntarredPath.exists()) {
      tempUntarredPath.mkdirs();
    }
    vip = StringUtil.join("://", "http", StringUtil.join(":", conf.getControllerVipHost(), conf.getControllerPort()));
    LOGGER.info("controller download url base is : " + vip);
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
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
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
        tmpSegmentDir =
            new File(tempUntarredPath, dataFile.getName() + "-" + conf.getControllerHost() + "_"
                + conf.getControllerPort() + "-" + System.currentTimeMillis());
        LOGGER.info("Untar segment to temp dir: " + tmpSegmentDir);
        if (tmpSegmentDir.exists()) {
          FileUtils.deleteDirectory(tmpSegmentDir);
        }
        if (!tmpSegmentDir.exists()) {
          tmpSegmentDir.mkdirs();
        }
        // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files in the segment in
        // order to ensure the segment is not corrupted
        TarGzCompressionUtils.unTar(dataFile, tmpSegmentDir);

        final SegmentMetadata metadata = new SegmentMetadataImpl(tmpSegmentDir.listFiles()[0]);
        final File resourceDir = new File(baseDataDir, metadata.getResourceName());
        File segmentFile = new File(resourceDir, dataFile.getName());
        if (segmentFile.exists()) {
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(dataFile, segmentFile);

        manager.addSegment(metadata, constructDownloadUrl(metadata.getResourceName(), dataFile.getName()));
        return new StringRepresentation("");
      } else {
        // Some problem occurs, sent back a simple line of text.
        rep = new StringRepresentation("no file uploaded", MediaType.TEXT_PLAIN);
        LOGGER.warn("No file was uploaded");
        setStatus(Status.SERVER_ERROR_INTERNAL);
      }
    } catch (final Exception e) {
      rep = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception in file upload", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    } finally {
      if ((tmpSegmentDir != null) && tmpSegmentDir.exists()) {
        try {
          FileUtils.deleteDirectory(tmpSegmentDir);
        } catch (final IOException e) {
          LOGGER.error("Caught exception in file upload", e);
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
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");
      LOGGER.info("Getting segment deletion request, resourceName: " + resourceName + " segmentName: " + segmentName);
      if (resourceName == null || segmentName == null) {
        throw new RuntimeException("either resource name or segment name is null");
      }
      PinotResourceManagerResponse res = null;
      if (ZKMetadataProvider.isSegmentExisted(manager.getPropertyStore(),
          TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(resourceName), segmentName)) {
        res = manager.deleteSegment(TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(resourceName), segmentName);
        rep = new StringRepresentation(res.toString());
      }
      if (ZKMetadataProvider.isSegmentExisted(manager.getPropertyStore(),
          TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(resourceName), segmentName)) {
        res = manager.deleteSegment(TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(resourceName), segmentName);
        rep = new StringRepresentation(res.toString());
      }
      if (res == null) {
        rep = new StringRepresentation("Cannot find the segment: " + segmentName + " in resource: " + resourceName);
      }
    } catch (final Exception e) {
      rep = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing delete request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return rep;
  }

  public static StringRepresentation exceptionToStringRepresentation(Exception e) {
    return new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
  }

  public String constructDownloadUrl(String resouceName, String segmentName) {
    final String ret = StringUtil.join("/", vip, "datafiles", resouceName, segmentName);
    return ret;
  }
}
