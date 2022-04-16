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
package org.apache.pinot.common.minion;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;


public class MinionTaskUtils {
  private MinionTaskUtils() {
  }

  public static String executeTask(HelixManager helixManager, AdhocTaskConfig adhocTaskConfig,
      Map<String, String> headers)
      throws IOException {
    try {
      InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(helixManager,
          CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + LeadControllerUtils.getHelixClusterLeader(
              helixManager));
      String url = "http://" + StringUtil.join("/", instanceConfig.getHostName() + ":" + instanceConfig.getPort(),
          "tasks/execute");
      headers.remove("content-length");
      String authToken = headers.get("Authorization");
      SimpleHttpResponse simpleHttpResponse = HttpClient.wrapAndThrowHttpException(
          HttpClient.getInstance()
              .sendJsonPostRequest(URI.create(url), adhocTaskConfig.toJsonString(), headers, authToken));
      return simpleHttpResponse.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }
}
