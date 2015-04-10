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
package com.linkedin.pinot.tools.admin.command;

import java.io.File;
import java.io.FileInputStream;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;

public class UploadDataCommand implements Command {
  @Option(name="-controllerHost", required=true, metaVar="<ControllerHostName>")
  String _controllerHost = null;

  @Option(name="-controllerPort", required=true, metaVar="<ControllerPort>")
  String _controllerPort = null;

  @Option(name="-segmentDir", required=true, metaVar="<segmentDir>")
  String _dataDir = null;

  public void init(String controllerHost, String controllerPort, String dataDir) {
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
    _dataDir = dataDir;
  }

  @Override
  public boolean execute() throws Exception {
    File dir = new File(_dataDir);
    File [] files = dir.listFiles();

    for (File file : files) {
      String srcDir = file.getAbsolutePath();

      String outFile = file.getAbsolutePath() + ".tar.gz";
      File tgzFile = new File(outFile);

      TarGzCompressionUtils.createTarGzOfDirectory(srcDir, outFile);
      FileUploadUtils.sendFile(_controllerHost, _controllerPort, tgzFile.getName(),
          new FileInputStream(tgzFile), tgzFile.length());
    }

    return true;
  }
}
