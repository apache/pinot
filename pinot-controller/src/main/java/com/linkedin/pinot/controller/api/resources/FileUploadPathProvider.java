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
import org.apache.commons.io.FileUtils;
import com.linkedin.pinot.controller.ControllerConf;


public class FileUploadPathProvider {
  private final ControllerConf _controllerConf;

  private final File _tmpDir;
  private final File _baseDataDir;
  private final File _tmpUntarredPath;

  public FileUploadPathProvider(ControllerConf controllerConf) throws InvalidControllerConfigException {
    _controllerConf = controllerConf;
    try {
      _baseDataDir = new File(_controllerConf.getDataDir());
      if (!_baseDataDir.exists()) {
        FileUtils.forceMkdir(_baseDataDir);
      }
      _tmpDir = new File(_baseDataDir, "fileUploadTemp");
      if (!_tmpDir.exists()) {
        FileUtils.forceMkdir(_tmpDir);
      }
      _tmpUntarredPath = new File(_tmpDir, "untarred");
      if (!_tmpUntarredPath.exists()) {
        _tmpUntarredPath.mkdirs();
      }
//      String vip = _controllerConf.generateVipUrl();
    } catch (Exception e) {
      throw new InvalidControllerConfigException("Bad controller configuration");
    }
  }

  public File getTmpDir() {
    return _tmpDir;
  }

  public File getBaseDataDir() {
    return _baseDataDir;
  }

  public File getTmpUntarredPath() {
    return _tmpUntarredPath;
  }
}
