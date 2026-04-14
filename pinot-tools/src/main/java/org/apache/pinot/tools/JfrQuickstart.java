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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.util.trace.ContinuousJfrStarter;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;


public class JfrQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Collections.singletonList("JFR");
  }

  @Override
  protected Map<String, String> getClusterConfigOverrides() {
    Map<String, String> clusterConfigOverrides = new HashMap<>(super.getClusterConfigOverrides());
    String jfrDirectory = System.getProperty("user.dir") + "/jfr";
    clusterConfigOverrides.put(CommonConstants.JFR + ".enabled", "true");
    clusterConfigOverrides.put(CommonConstants.JFR + "." + ContinuousJfrStarter.DIRECTORY, jfrDirectory);
    clusterConfigOverrides.put(CommonConstants.JFR + ".dumpPath", jfrDirectory);
    clusterConfigOverrides.put(CommonConstants.JFR + ".preserveRepository", "true");
    return clusterConfigOverrides;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new JfrQuickstart().execute();
  }
}
