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
package com.linkedin.pinot.tools.admin.command;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for command to upload data into Pinot.
 *
 *
 */
public class UploadSegmentCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(UploadSegmentCommand.class);
  private static final String SEGMENT_UPLOADER = "segmentUploader";
  private static final String TAR_GZIP = ".tar.gz";

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-segmentDir", required = true, metaVar = "<string>", usage = "Path to segment directory.")
  private String _segmentDir = null;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "UploadSegment";
  }

  @Override
  public String toString() {
    return ("UploadSegment -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -segmentDir " + _segmentDir);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Upload the Pinot segments.";
  }

  public UploadSegmentCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public UploadSegmentCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public UploadSegmentCommand setSegmentDir(String segmentDir) {
    _segmentDir = segmentDir;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }

    // Create a temporary working directory.
    File tempDir = File.createTempFile(SEGMENT_UPLOADER, null, FileUtils.getTempDirectory());
    FileUtils.deleteQuietly(tempDir);
    FileUtils.forceMkdir(tempDir);

    LOGGER.info("Executing command: " + toString());
    File dir = new File(_segmentDir);
    File[] files = dir.listFiles();
    Preconditions.checkNotNull(files);

    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      URI uploadSegmentHttpURI =
          FileUploadDownloadClient.getUploadSegmentHttpURI(_controllerHost, Integer.parseInt(_controllerPort));
      for (File file : files) {
        File tgzFile = file;

        if (file.isDirectory()) {
          LOGGER.info("Compressing segment {}", file.getName());

          String srcDir = file.getAbsolutePath();
          String tgzFileName = TarGzCompressionUtils.createTarGzOfDirectory(srcDir,
              tempDir.getAbsolutePath() + File.separator + file.getName() + TAR_GZIP);
          tgzFile = new File(tgzFileName);
        }

        LOGGER.info("Uploading segment {}", tgzFile.getName());
        fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, tgzFile.getName(), tgzFile);
      }
    } finally {
      // Delete the temporary working directory.
      FileUtils.deleteQuietly(tempDir);
    }
    return true;
  }
}
