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
package org.apache.pinot.tools;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public abstract class QuickStartBase {
  protected File _dataDir = FileUtils.getTempDirectory();
  protected String _zkExternalAddress;

  public QuickStartBase setDataDir(String dataDir) {
    _dataDir = new File(dataDir);
    return this;
  }

  public QuickStartBase setZkExternalAddress(String zkExternalAddress) {
    _zkExternalAddress = zkExternalAddress;
    return this;
  }

  public abstract List<String> types();

  protected void waitForBootstrapToComplete(QuickstartRunner runner)
      throws Exception {
    QuickStartBase.printStatus(Quickstart.Color.CYAN,
        "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);
  }

  public static void printStatus(Quickstart.Color color, String message) {
    System.out.println(color.getCode() + message + Quickstart.Color.RESET.getCode());
  }

  public abstract void execute()
      throws Exception;
}
