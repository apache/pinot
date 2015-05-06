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
package com.linkedin.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;


public class ControllerHelixHelper {
  public static final String BROKER_RESOURCE = CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;

  public static BrokerDataResource getBrokerDataResource(HelixAdmin admin, String clusterName, String dataResourceName) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(clusterName, BROKER_RESOURCE, admin);
    return BrokerDataResource.fromMap(configs, dataResourceName);
  }

  public static BrokerTagResource getBrokerTag(HelixAdmin admin, String clusterName, String tag) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(clusterName, BROKER_RESOURCE, admin);
    return BrokerTagResource.fromMap(configs, tag);
  }

  public static List<String> getBrokerTagList(HelixAdmin admin, String clusterName) {
    final Set<String> brokerTagSet = new HashSet<String>();
    Map<String, String> brokerResourceConfig = HelixHelper.getBrokerResourceConfig(admin, clusterName);
    for (String key : brokerResourceConfig.keySet()) {
      if (key.startsWith(BrokerTagResource.CONFIG_PREFIX_OF_BROKER_TAG) && key.endsWith(".tag")) {
        brokerTagSet.add(brokerResourceConfig.get(key));
      }
    }
    final List<String> brokerTagList = new ArrayList<String>();
    brokerTagList.addAll(brokerTagSet);
    return brokerTagList;
  }

  public static void updateBrokerTag(HelixAdmin admin, String clusterName, BrokerTagResource brokerTag) {
    Map<String, String> brokerResourceConfig = HelixHelper.getBrokerResourceConfig(admin, clusterName);
    brokerResourceConfig.putAll(brokerTag.toBrokerConfigs());
    HelixHelper.updateBrokerConfig(brokerResourceConfig, admin, clusterName);
  }

  public static void updateBrokerDataResource(HelixAdmin admin, String clusterName,
      BrokerDataResource brokerDataResource) {
    Map<String, String> brokerResourceConfig = HelixHelper.getBrokerResourceConfig(admin, clusterName);
    brokerResourceConfig.putAll(brokerDataResource.toBrokerConfigs());
    HelixHelper.updateBrokerConfig(brokerResourceConfig, admin, clusterName);
  }

  public static List<String> getDataResourceListFromBrokerResourceConfig(HelixAdmin admin, String clusterName) {
    List<String> dataResourceList = new ArrayList<String>();
    Map<String, String> brokerResourceConfig = HelixHelper.getBrokerResourceConfig(admin, clusterName);
    for (String key : brokerResourceConfig.keySet()) {
      if (key.startsWith(BrokerDataResource.CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE) && key.endsWith(".resourceName")) {
        dataResourceList.add(brokerResourceConfig.get(key));
      }
    }
    return dataResourceList;
  }

  public static void deleteBrokerTagFromResourceConfig(HelixAdmin admin, String clusterName, String brokerTag) {
    BrokerTagResource brokerTagResource = new BrokerTagResource(0, brokerTag);
    for (String key : brokerTagResource.toBrokerConfigs().keySet()) {
      HelixHelper.deleteResourcePropertyFromHelix(admin, clusterName, BROKER_RESOURCE, key);
    }
    List<String> dataResourceList = getDataResourceListFromBrokerResourceConfig(admin, clusterName);
    for (String dataResource : dataResourceList) {
      if (isBrokerDataResourceTagAs(admin, clusterName, dataResource, brokerTag)) {
        deleteBrokerDataResourceConfig(admin, clusterName, dataResource);
      }
    }
  }

  public static boolean isBrokerDataResourceTagAs(HelixAdmin admin, String clusterName, String dataResource,
      String brokerTag) {
    Map<String, String> brokerResourceConfig = HelixHelper.getBrokerResourceConfig(admin, clusterName);
    return brokerResourceConfig.get(BrokerDataResource.CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + dataResource + ".tag")
        .equals(brokerTag);
  }

  public static void deleteBrokerDataResourceConfig(HelixAdmin admin, String clusterName, String brokerDataResourceName) {
    BrokerDataResource brokerDataResource = new BrokerDataResource(brokerDataResourceName, 0, "");
    for (String key : brokerDataResource.toBrokerConfigs().keySet()) {
      HelixHelper.deleteResourcePropertyFromHelix(admin, clusterName, BROKER_RESOURCE, key);
    }
    BrokerTagResource brokerTagResource = new BrokerTagResource(0, "broker_" + brokerDataResourceName);
    for (String key : brokerTagResource.toBrokerConfigs().keySet()) {
      HelixHelper.deleteResourcePropertyFromHelix(admin, clusterName, BROKER_RESOURCE, key);
    }
  }
}
