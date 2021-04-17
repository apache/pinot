/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.admin.command;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
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

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-controllerProtocol", required = false, metaVar = "<String>", usage = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

  @Option(name = "-segmentDir", required = true, metaVar = "<string>", usage = "Path to segment directory.")
  private String _segmentDir = null;

  // TODO: make this as a required field once we deprecate the table name from segment metadata
  @Option(name = "-tableName", required = false, metaVar = "<string>", usage = "Table name to upload.")
  private String _tableName = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
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
    return ("UploadSegment -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort + " -segmentDir " + _segmentDir);
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

  public UploadSegmentCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public UploadSegmentCommand setUser(String user) {
    _user = user;
    return this;
  }

  public UploadSegmentCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public UploadSegmentCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public UploadSegmentCommand setSegmentDir(String segmentDir) {
    _segmentDir = segmentDir;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    // Create a temporary working directory.
    File tempDir = File.createTempFile(SEGMENT_UPLOADER, null, FileUtils.getTempDirectory());
    FileUtils.deleteQuietly(tempDir);
    FileUtils.forceMkdir(tempDir);

    LOGGER.info("Executing command: {}", toString());
    File segmentDir = new File(_segmentDir);
    File[] segmentFiles = segmentDir.listFiles();
    Preconditions.checkNotNull(segmentFiles);

    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentURI(_controllerProtocol, _controllerHost,
          Integer.parseInt(_controllerPort));
      for (File segmentFile : segmentFiles) {
        File segmentTarFile;
        if (segmentFile.isDirectory()) {
          // Tar the segment directory
          String segmentName = segmentFile.getName();
          LOGGER.info("Compressing segment: {}", segmentName);
          segmentTarFile = new File(tempDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
          TarGzCompressionUtils.createTarGzFile(segmentFile, segmentTarFile);
        } else {
          segmentTarFile = segmentFile;
        }

        LOGGER.info("Uploading segment tar file: {}", segmentTarFile);
        fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
            makeAuthHeader(makeAuthToken(_authToken, _user, _password)),
            Collections
                .singletonList(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, _tableName)),
            FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
      }
    } finally {
      // Delete the temporary working directory.
      FileUtils.deleteQuietly(tempDir);
    }
    return true;
  }
}
