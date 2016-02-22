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
package com.linkedin.pinot.controller.helix;

import java.io.File;
import org.apache.commons.io.FileUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;


/**
 * Utilities to start controllers during unit tests.
 *
 */
public class ControllerTestUtils {
  public static final String DEFAULT_CONTROLLER_INSTANCE_NAME = "localhost_11984";
  public static final String DEFAULT_DATA_DIR = FileUtils.getTempDirectoryPath() + File.separator + "test-controller-" + System.currentTimeMillis();
  public static final String DEFAULT_CONTROLLER_API_PORT = "8998";
  public static final String DEFAULT_CONTROLLER_HOST = "localhost";

  public static ControllerConf getDefaultControllerConfiguration() {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost(DEFAULT_CONTROLLER_INSTANCE_NAME);
    conf.setControllerPort(DEFAULT_CONTROLLER_API_PORT);
    conf.setDataDir(DEFAULT_DATA_DIR);
    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol("http");

    return conf;
  }

  public static ControllerStarter startController(final String clusterName, final String zkStr, final ControllerConf configuration) {
    configuration.setHelixClusterName(clusterName);
    configuration.setZkStr(zkStr);

    ControllerStarter controllerStarter = new ControllerStarter(configuration);
    controllerStarter.start();
    return controllerStarter;
  }

  public static void stopController(final ControllerStarter controllerStarter) {
    controllerStarter.stop();
    FileUtils.deleteQuietly(new File(DEFAULT_DATA_DIR));
  }
}
