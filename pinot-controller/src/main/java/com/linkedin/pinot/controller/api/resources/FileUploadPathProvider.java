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

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO This is a misc class used by many jersey apis. Need to rename it correctly
public class FileUploadPathProvider {
  public final static String STATE = "state";
  public final static String TABLE_NAME = "tableName";
  private final static String DEFAULT_SCHEME = "file";
  private final static String FILE_UPLOAD_TEMP_PATH = "/fileUploadTemp";
  private final static String UNTARRED_PATH = "/untarred";
  private final static String SCHEMAS_TEMP = "/schemasTemp";
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadPathProvider.class);

  private final ControllerConf _controllerConf;

  private final URI _fileUploadTmpDirURI;
  private final URI _baseDataDirURI;
  private final URI _tmpUntarredPathURI;
  private final URI _schemasTmpDirURI;
  private final String _vip;

  public FileUploadPathProvider(ControllerConf controllerConf) throws InvalidControllerConfigException {
    _controllerConf = controllerConf;

    // Pick the correct scheme for PinotFS. For pluggable storage, we will expect that the directory is configured
    // with a storage prefix. For backwards compatibility, we will assume local/mounted NFS storage if nothing is configured.
    String scheme = DEFAULT_SCHEME;
    String dataDir = _controllerConf.getDataDir();
    try {
      URI uri = new URI(_controllerConf.getDataDir());
      String dataDirScheme = uri.getScheme();
      if (dataDirScheme ==  null) {
        // Assume local fs
        dataDir = "file://" + _controllerConf.getDataDir();
      } else {
        scheme = dataDirScheme;
      }
    } catch (URISyntaxException e) {
      LOGGER.error("Invalid path set as data dir {}", _controllerConf.getDataDir());
      Utils.rethrowException(e);
    }

    PinotFS pinotFS = PinotFSFactory.create(scheme);

    try {
      _baseDataDirURI = new URI(dataDir);
      if (!pinotFS.exists(_baseDataDirURI)) {
        pinotFS.mkdir(_baseDataDirURI);
      }
      _fileUploadTmpDirURI = new URI(_baseDataDirURI + FILE_UPLOAD_TEMP_PATH);
      if (!pinotFS.exists(_fileUploadTmpDirURI)) {
        pinotFS.mkdir(_fileUploadTmpDirURI);
      }
      _tmpUntarredPathURI = new URI(_fileUploadTmpDirURI + UNTARRED_PATH);
      if (!pinotFS.exists(_tmpUntarredPathURI)) {
        pinotFS.mkdir(_tmpUntarredPathURI);
      }
      _schemasTmpDirURI = new URI(_baseDataDirURI + SCHEMAS_TEMP);
      if (!pinotFS.exists(_schemasTmpDirURI)) {
        pinotFS.mkdir(_schemasTmpDirURI);
      }
      _vip = _controllerConf.generateVipUrl();
    } catch (Exception e) {
      throw new InvalidControllerConfigException("Bad controller configuration", e);
    }
  }

  public String getVip() {
    return _vip;
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
