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
package org.apache.pinot.controller.api.resources;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerFilePathProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerFilePathProvider.class);
  private static final String FILE_UPLOAD_TEMP_DIR = "fileUploadTemp";
  private static final String UNTARRED_FILE_TEMP_DIR = "untarredFileTemp";
  private static final String FILE_DOWNLOAD_TEMP_DIR = "fileDownloadTemp";

  private static ControllerFilePathProvider _instance;

  /**
   * NOTE: this should be called only once when starting the controller. We don't check whether _instance is null
   * because we might start multiple controllers in the same JVM for testing.
   */
  public static void init(ControllerConf controllerConf)
      throws InvalidControllerConfigException {
    _instance = new ControllerFilePathProvider(controllerConf);
  }

  public static ControllerFilePathProvider getInstance() {
    Preconditions.checkState(_instance != null, "ControllerFilePathProvider has not been initialized");
    return _instance;
  }

  private final URI _dataDirURI;
  private final File _fileUploadTempDir;
  private final File _untarredFileTempDir;
  private final File _fileDownloadTempDir;
  private final String _vip;

  private ControllerFilePathProvider(ControllerConf controllerConf)
      throws InvalidControllerConfigException {
    String dataDir = controllerConf.getDataDir();
    try {
      _dataDirURI = URIUtils.getUri(dataDir);
      LOGGER.info("Data directory: {}", _dataDirURI);

      PinotFS pinotFS = PinotFSFactory.create(_dataDirURI.getScheme());
      if (pinotFS.exists(_dataDirURI)) {
        Preconditions
            .checkState(pinotFS.isDirectory(_dataDirURI), "Data directory: %s must be a directory", _dataDirURI);
      } else {
        Preconditions.checkState(pinotFS.mkdir(_dataDirURI), "Failed to create data directory: %s", _dataDirURI);
      }

      String localTempDirPath = controllerConf.getLocalTempDir();
      File localTempDir;
      if (localTempDirPath == null) {
        Preconditions.checkState(_dataDirURI.getScheme().equalsIgnoreCase(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME),
            "Local temporary directory is not configured, cannot use remote data directory: %s as local temporary "
                + "directory", _dataDirURI);
        // NOTE: use instance id as the directory name so that each controller gets its own temporary directory in case
        // of shared file system
        localTempDir = new File(new File(_dataDirURI),
            controllerConf.getControllerHost() + "_" + controllerConf.getControllerPort());
      } else {
        localTempDir = new File(localTempDirPath);
      }
      LOGGER.info("Local temporary directory: {}", localTempDir);

      _fileUploadTempDir = new File(localTempDir, FILE_UPLOAD_TEMP_DIR);
      LOGGER.info("File upload temporary directory: {}", _fileUploadTempDir);
      initDir(_fileUploadTempDir);

      _untarredFileTempDir = new File(localTempDir, UNTARRED_FILE_TEMP_DIR);
      LOGGER.info("Untarred file temporary directory: {}", _untarredFileTempDir);
      initDir(_untarredFileTempDir);

      _fileDownloadTempDir = new File(localTempDir, FILE_DOWNLOAD_TEMP_DIR);
      LOGGER.info("File download temporary directory: {}", _fileDownloadTempDir);
      initDir(_fileDownloadTempDir);

      _vip = controllerConf.generateVipUrl();
    } catch (Exception e) {
      throw new InvalidControllerConfigException("Caught exception while initializing file upload path provider", e);
    }
  }

  private void initDir(File dir)
      throws IOException {
    if (dir.exists()) {
      FileUtils.cleanDirectory(dir);
    } else {
      FileUtils.forceMkdir(dir);
    }
  }

  public String getVip() {
    return _vip;
  }

  public URI getDataDirURI() {
    return _dataDirURI;
  }

  public File getFileUploadTempDir() {
    if (!Files.exists(_fileUploadTempDir.toPath())) {
      try {
        Files.createDirectories(_fileUploadTempDir.toPath());
      } catch (FileAlreadyExistsException ignored) {
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return _fileUploadTempDir;
  }

  public File getUntarredFileTempDir() {
    if (!Files.exists(_untarredFileTempDir.toPath())) {
      try {
        Files.createDirectories(_untarredFileTempDir.toPath());
      } catch (FileAlreadyExistsException ignored) {
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return _untarredFileTempDir;
  }

  public File getFileDownloadTempDir() {
    if (!Files.exists(_fileDownloadTempDir.toPath())) {
      try {
        Files.createDirectories(_fileDownloadTempDir.toPath());
      } catch (FileAlreadyExistsException ignored) {
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return _fileDownloadTempDir;
  }
}
