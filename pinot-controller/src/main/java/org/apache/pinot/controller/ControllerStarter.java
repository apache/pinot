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
package org.apache.pinot.controller;

import java.io.File;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Default controller startable implementation. Contains methods to start and stop the controller
 */
public class ControllerStarter extends BaseControllerStarter {

  public ControllerStarter() {
  }

  @Deprecated
  public ControllerStarter(PinotConfiguration pinotConfiguration)
      throws Exception {
    init(pinotConfiguration);
  }

  public static ControllerStarter startDefault()
      throws Exception {
    return startDefault(null);
  }

  public static ControllerStarter startDefault(File webappPath)
      throws Exception {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost("localhost");
    conf.setControllerPort("9000");
    conf.setDataDir("/tmp/PinotController");
    conf.setZkStr("localhost:2122");
    conf.setHelixClusterName("quickstart");
    if (webappPath == null) {
      String path = ControllerStarter.class.getClassLoader().getResource("webapp").getFile();
      if (!path.startsWith("file://")) {
        path = "file://" + path;
      }
      conf.setQueryConsolePath(path);
    } else {
      conf.setQueryConsolePath("file://" + webappPath.getAbsolutePath());
    }

    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol(CommonConstants.HTTP_PROTOCOL);
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setOfflineSegmentIntervalCheckerFrequencyInSeconds(3600);
    conf.setRealtimeSegmentValidationFrequencyInSeconds(3600);
    conf.setBrokerResourceValidationFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5 * 60);
    conf.setSegmentRelocatorFrequencyInSeconds(3600);
    conf.setStatusCheckerWaitForPushTimeInSeconds(10 * 60);
    conf.setTenantIsolationEnabled(true);

    final ControllerStarter starter = new ControllerStarter();
    starter.init(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
