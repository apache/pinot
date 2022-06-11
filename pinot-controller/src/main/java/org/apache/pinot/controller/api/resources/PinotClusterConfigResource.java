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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.services.PinotClusterConfigService;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotClusterConfigResource implements PinotClusterConfigService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterConfigResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Override
  public String getClusterInfo() {
    ObjectNode ret = JsonUtils.newObjectNode();
    ret.put("clusterName", _pinotHelixResourceManager.getHelixClusterName());
    return ret.toString();
  }

  @Override
  public String listClusterConfigs() {
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(_pinotHelixResourceManager.getHelixClusterName()).build();
    List<String> configKeys = helixAdmin.getConfigKeys(configScope);
    ObjectNode ret = JsonUtils.newObjectNode();
    Map<String, String> configs = helixAdmin.getConfig(configScope, configKeys);
    for (String key : configs.keySet()) {
      ret.put(key, configs.get(key));
    }
    return ret.toString();
  }


  @Override
  public SuccessResponse updateClusterConfig(String body) {
    try {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(body);
      HelixAdmin admin = _pinotHelixResourceManager.getHelixAdmin();
      HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
          .forCluster(_pinotHelixResourceManager.getHelixClusterName()).build();
      Iterator<String> fieldNamesIterator = jsonNode.fieldNames();
      while (fieldNamesIterator.hasNext()) {
        String key = fieldNamesIterator.next();
        String value = jsonNode.get(key).textValue();
        admin.setConfig(configScope, Collections.singletonMap(key, value));
      }
      return new SuccessResponse("Updated cluster config.");
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Error converting request to cluster config.",
          Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to update cluster config.",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @Override
  public SuccessResponse deleteClusterConfig(String configName) {
    try {
      HelixAdmin admin = _pinotHelixResourceManager.getHelixAdmin();
      HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
          .forCluster(_pinotHelixResourceManager.getHelixClusterName()).build();
      admin.removeConfig(configScope, Arrays.asList(configName));
      return new SuccessResponse("Deleted cluster config: " + configName);
    } catch (Exception e) {
      String errStr = "Failed to delete cluster config: " + configName;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
