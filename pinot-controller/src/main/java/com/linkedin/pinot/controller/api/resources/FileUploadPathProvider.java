/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.filesystem.LocalPinotFS;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO This is a misc class used by many jersey apis. Need to rename it correctly
public class FileUploadPathProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadPathProvider.class);

  public final static String STATE = "state";
  public final static String TABLE_NAME = "tableName";

  private final static String FILE_UPLOAD_TEMP_PATH = "/fileUploadTemp";
  private final static String UNTARRED_PATH = "/untarred";
  private final static String SCHEMAS_TEMP = "/schemasTemp";

  private final URI _baseDataDirURI;
  private final URI _schemasTmpDirURI;
  private final URI _localTempDirURI;
  private final URI _fileUploadTmpDirURI;
  private final URI _tmpUntarredPathURI;
  private final String _vip;

  public FileUploadPathProvider(ControllerConf controllerConf) throws InvalidControllerConfigException {
    String dataDir = controllerConf.getDataDir();
    try {
      // URIs that are allowed to be remote
      _baseDataDirURI = ControllerConf.getUriFromPath(dataDir);
      LOGGER.info("Data directory: {}", _baseDataDirURI);
      _schemasTmpDirURI = new URI(_baseDataDirURI + SCHEMAS_TEMP);
      LOGGER.info("Schema temporary directory: {}", _schemasTmpDirURI);
      String scheme = _baseDataDirURI.getScheme();
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      mkdirIfNotExists(pinotFS, _baseDataDirURI);
      mkdirIfNotExists(pinotFS, _schemasTmpDirURI);

      // URIs that are always local
      String localTempDir = controllerConf.getLocalTempDir();
      if (localTempDir == null) {
        LOGGER.info("Local temporary directory is not configured, use data directory as the local temporary directory");
        _localTempDirURI = _baseDataDirURI;
      } else {
        _localTempDirURI = ControllerConf.getUriFromPath(localTempDir);
      }
      LOGGER.info("Local temporary directory: {}", _localTempDirURI);
      if (!_localTempDirURI.getScheme().equalsIgnoreCase(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME)) {
        throw new IllegalStateException("URI scheme must be file for local temporary directory: " + _localTempDirURI);
      }
      _fileUploadTmpDirURI = new URI(_localTempDirURI + FILE_UPLOAD_TEMP_PATH);
      LOGGER.info("File upload temporary directory: {}", _fileUploadTmpDirURI);
      _tmpUntarredPathURI = new URI(_fileUploadTmpDirURI + UNTARRED_PATH);
      LOGGER.info("Untarred file temporary directory: {}", _tmpUntarredPathURI);
      PinotFS localPinotFS = new LocalPinotFS();
      mkdirIfNotExists(localPinotFS, _localTempDirURI);
      mkdirIfNotExists(localPinotFS, _fileUploadTmpDirURI);
      mkdirIfNotExists(localPinotFS, _tmpUntarredPathURI);

      _vip = controllerConf.generateVipUrl();
    } catch (Exception e) {
      throw new InvalidControllerConfigException("Caught exception while initializing file upload path provider", e);
    }
  }

  private static void mkdirIfNotExists(PinotFS pinotFS, URI uri) throws IOException {
    if (!pinotFS.exists(uri)) {
      if (!pinotFS.mkdir(uri)) {
        throw new IOException("Failed to create directory at URI: " + uri);
      }
    }
  }

  public String getVip() {
    return _vip;
  }

  public URI getLocalTempDirURI() {
    return _localTempDirURI;
  }

  public URI getFileUploadTmpDirURI() {
    return _fileUploadTmpDirURI;
  }

  public URI getBaseDataDirURI() {
    return _baseDataDirURI;
  }

  public URI getTmpUntarredPathURI() {
    return _tmpUntarredPathURI;
  }

  public URI getSchemasTmpDirURI() {
    return _schemasTmpDirURI;
  }

  // TODO: The following getters that return files should eventually be migrated to URIs once pluggable storage support
  // is complete throughout the system. Leaving this here for backwards compatibility for now.
  public File getFileUploadTmpDir() {
    return new File(_fileUploadTmpDirURI);
  }

  public File getBaseDataDir() {
    return new File(_baseDataDirURI);
  }

  public File getTmpUntarredPath() {
    return new File(_tmpUntarredPathURI);
  }

  public File getSchemasTmpDir() {
    return new File(_schemasTmpDirURI);
  }
}
