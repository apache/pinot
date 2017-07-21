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

package com.linkedin.pinot.controller.api.resources;

import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.ControllerConf;


// TODO This is a misc class used by many jersey apis. Need to rename it correctly
public class FileUploadPathProvider {
  public static final String HDR_CONTROLLER_VERSION = "Pinot-Controller-Version";
  public final static String HDR_CONTROLLER_HOST = "Pinot-Controller-Host";

  private final ControllerConf _controllerConf;

  private final File _fileUploadTmpDir;
  private final File _baseDataDir;
  private final File _tmpUntarredPath;
  private final File _schemasTmpDir;
  private final String _vip;

  private final static String CONTROLLER_COMPONENT = "pinot-controller";

  private volatile static String controllerHostName = null;
  private volatile static String controllerVersion = null;

  public FileUploadPathProvider(ControllerConf controllerConf) throws InvalidControllerConfigException {
    _controllerConf = controllerConf;
    try {
      _baseDataDir = new File(_controllerConf.getDataDir());
      if (!_baseDataDir.exists()) {
        FileUtils.forceMkdir(_baseDataDir);
      }
      _fileUploadTmpDir = new File(_baseDataDir, "fileUploadTemp");
      if (!_fileUploadTmpDir.exists()) {
        FileUtils.forceMkdir(_fileUploadTmpDir);
      }
      _tmpUntarredPath = new File(_fileUploadTmpDir, "untarred");
      if (!_tmpUntarredPath.exists()) {
        _tmpUntarredPath.mkdirs();
      }
      _schemasTmpDir = new File(_baseDataDir, "schemasTemp");
      if (!_schemasTmpDir.exists()) {
        FileUtils.forceMkdir(_schemasTmpDir);
      }
      _vip = _controllerConf.generateVipUrl();
    } catch (Exception e) {
      throw new InvalidControllerConfigException("Bad controller configuration");
    }
  }

  public File getFileUploadTmpDir() {
    return _fileUploadTmpDir;
  }

  public File getBaseDataDir() {
    return _baseDataDir;
  }

  public File getTmpUntarredPath() {
    return _tmpUntarredPath;
  }

  public String getVip() {
    return _vip;
  }

  public File getSchemasTmpDir() {
    return _schemasTmpDir;
  }

  public static String getControllerHostName() {
    if (controllerHostName != null) {
      return controllerHostName;
    }

    controllerHostName = NetUtil.getHostnameOrAddress();
    if (controllerHostName == null) {
      // In case of a temporary failure, we will go back to getting the right value again.
      return "unknown";
    }
    return controllerHostName;
  }

  public static String getHdrControllerVersion() {
    if (controllerVersion != null) {
      return controllerVersion;
    }
    Map<String, String> versions = Utils.getComponentVersions();
    controllerVersion = versions.get(CONTROLLER_COMPONENT);
    if (controllerVersion == null) {
      controllerVersion = "Unknown";
    }
    return controllerVersion;
  }
}
