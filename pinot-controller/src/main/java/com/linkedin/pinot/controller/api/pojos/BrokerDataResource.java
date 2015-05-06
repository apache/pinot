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
package com.linkedin.pinot.controller.api.pojos;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * BrokerDataResource is used for broker to take care which data resource to serve.
 *
 * @author xiafu
 *
 */
public class BrokerDataResource {

  public static final String CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE = "broker.dataResource.";
  private final String resourceName;
  private final int numBrokerInstances;
  private final String tag;

  @JsonCreator
  public BrokerDataResource(
      @JsonProperty(CommonConstants.Broker.DataResource.RESOURCE_NAME) String resourceName,
      @JsonProperty(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES) int numBrokerInstances,
      @JsonProperty(CommonConstants.Broker.DataResource.TAG) String tag) {
    this.resourceName = resourceName;
    this.numBrokerInstances = numBrokerInstances;
    this.tag = tag;
  }

  public BrokerDataResource(String dataResorceName, BrokerTagResource tagRequest) {
    resourceName = dataResorceName;
    tag = tagRequest.getTag();
    numBrokerInstances = tagRequest.getNumBrokerInstances();
  }

  public String getResourceName() {
    return resourceName;
  }

  public int getNumBrokerInstances() {
    return numBrokerInstances;
  }

  public String getTag() {
    return tag;
  }

  public static BrokerDataResource fromMap(Map<String, String> props) {
    return new BrokerDataResource(
        props.get(CommonConstants.Broker.DataResource.RESOURCE_NAME),
        Integer.parseInt(props.get(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES)),
        props.get(CommonConstants.Broker.DataResource.TAG));
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Broker.DataResource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, numBrokerInstances + "");
    props.put(CommonConstants.Broker.DataResource.TAG, tag);
    return props;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("resourceName : " + resourceName + "\n");
    bld.append("numBrokerInstances : " + numBrokerInstances + "\n");
    bld.append("tag : " + tag + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(CommonConstants.Broker.DataResource.RESOURCE_NAME, resourceName);
    ret.put(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, numBrokerInstances);
    ret.put(CommonConstants.Broker.DataResource.TAG, tag);
    return ret;
  }

  public static void main(String[] args) {
    final Map<String, String> configs = new HashMap<String, String>();
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1." + CommonConstants.Broker.DataResource.RESOURCE_NAME, "r1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1." + CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, "1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1." + CommonConstants.Broker.DataResource.TAG, "tag0");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2." + CommonConstants.Broker.DataResource.RESOURCE_NAME, "r2");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2." + CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, "2");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2." + CommonConstants.Broker.DataResource.TAG, "tag1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3." + CommonConstants.Broker.DataResource.RESOURCE_NAME, "r3");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3." + CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, "3");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3." + CommonConstants.Broker.DataResource.TAG, "tag1");
    System.out.println(fromMap(configs, "r1"));
    System.out.println(fromMap(configs, "r2"));
    System.out.println(fromMap(configs, "r3"));

    System.out.println(fromMap(configs, "r1").toBrokerConfigs());
    System.out.println(fromMap(configs, "r2").toBrokerConfigs());
    System.out.println(fromMap(configs, "r3").toBrokerConfigs());
  }

  public static BrokerDataResource fromMap(Map<String, String> configs, String resourceName) {
    final Map<String, String> resourceBrokerConfig = new HashMap<String, String>();
    for (final String key : configs.keySet()) {
      if (key.startsWith(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName)) {
        resourceBrokerConfig.put(key.split(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + ".", 2)[1], configs.get(key));
      }
    }
    return BrokerDataResource.fromMap(resourceBrokerConfig);
  }

  public Map<String, String> toBrokerConfigs() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + "." + CommonConstants.Broker.DataResource.RESOURCE_NAME, resourceName);
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + "." + CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, numBrokerInstances + "");
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + "." + CommonConstants.Broker.DataResource.TAG, tag);
    return props;
  }
}
