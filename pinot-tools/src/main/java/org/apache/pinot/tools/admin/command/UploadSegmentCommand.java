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
import java.io.FileInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to upload data into Pinot.
 *
 *
 */
@CommandLine.Command(name = "UploadSegment", mixinStandardHelpOptions = true)
public class UploadSegmentCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(UploadSegmentCommand.class);
  private static final String SEGMENT_UPLOADER = "segmentUploader";

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "Host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "Protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  @CommandLine.Option(names = {"-segmentDir"}, required = true, description = "Path to segment directory.")
  private String _segmentDir = null;

  @CommandLine.Option(names = {"-tableName"}, required = false, description = "Table name to upload")
  private String _tableName = null;

  @CommandLine.Option(names = {"-tableType"}, required = false,
      description = "Table type to upload. Can be OFFLINE or REALTIME")
  private TableType _tableType = TableType.OFFLINE;

  @CommandLine.Option(names = {"-customMetadata"}, required = false, split = ",",
      description = "Custom metadata to add to segment ZK metadata in key=value format (e.g. key1=value1,key2=value2)")
  private String[] _customMetadata = null;

  @CommandLine.Option(names = {"-customMetadataMode"}, required = false,
      description = "Mode for custom metadata modification. Can be REPLACE or UPDATE. Default is UPDATE")
  private SegmentZKMetadataCustomMapModifier.ModifyMode _customMetadataMode =
      SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE;

  private AuthProvider _authProvider;

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

  public UploadSegmentCommand setSegmentDir(String segmentDir) {
    _segmentDir = segmentDir;
    return this;
  }

  public UploadSegmentCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  public UploadSegmentCommand setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public void setTableType(TableType tableType) {
    _tableType = tableType;
  }

  public UploadSegmentCommand setCustomMetadata(String[] customMetadata) {
    _customMetadata = customMetadata;
    return this;
  }

  public UploadSegmentCommand setCustomMetadataMode(SegmentZKMetadataCustomMapModifier.ModifyMode customMetadataMode) {
    _customMetadataMode = customMetadataMode;
    return this;
  }

  private Map<String, String> parseCustomMetadata() {
    if (_customMetadata == null || _customMetadata.length == 0) {
      return null;
    }
    Map<String, String> customMetadataMap = new java.util.HashMap<>();
    for (String keyValue : _customMetadata) {
      String[] parts = keyValue.split("=", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid custom metadata format. Expected key=value, got: " + keyValue);
      }
      customMetadataMap.put(parts[0], parts[1]);
    }
    return customMetadataMap;
  }

  @Override
  public boolean execute()
      throws Exception {
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
      URI uploadSegmentHttpURI = FileUploadDownloadClient
          .getUploadSegmentURI(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort));
      for (File segmentFile : segmentFiles) {
        File segmentTarFile;
        if (segmentFile.isDirectory()) {
          // Tar the segment directory
          String segmentName = segmentFile.getName();
          LOGGER.info("Compressing segment: {}", segmentName);
          segmentTarFile = new File(tempDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
          TarCompressionUtils.createCompressedTarFile(segmentFile, segmentTarFile);
        } else {
          segmentTarFile = segmentFile;
        }

        LOGGER.info("Uploading segment tar file: {}", segmentTarFile);
        List<Header> headerList =
            AuthProviderUtils.makeAuthHeaders(
                AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password));

        // Add custom metadata header if provided
        Map<String, String> customMetadataMap = parseCustomMetadata();
        if (customMetadataMap != null && !customMetadataMap.isEmpty()) {
          SegmentZKMetadataCustomMapModifier modifier = new SegmentZKMetadataCustomMapModifier(_customMetadataMode,
              customMetadataMap);
          headerList.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
              modifier.toJsonString()));
          LOGGER.info("Added custom metadata modifier: {}", modifier.toJsonString());
        }

        FileInputStream fileInputStream = new FileInputStream(segmentTarFile);
        fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(),
            fileInputStream, headerList, null, _tableName, _tableType);
      }
    } finally {
      // Delete the temporary working directory.
      FileUtils.deleteQuietly(tempDir);
    }
    return true;
  }
}
