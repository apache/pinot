/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.pinot.common.utils.CommonConstants;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Instance POJO, used as part of the API to create instances.
 */
//@Example("{\n" + "\t\"host\": \"hostname.example.com\",\n" + "\t\"port\": \"1234\",\n" + "\t\"type\": \"server\"\n" + "}")
public class Instance {
  private final String _host;
  private final String _port;
  private final String _type;
  private final String _tag;
  private final String _instancePrefix;

  public static Instance fromInstanceConfig(InstanceConfig instanceConfig) {
    InstanceConfig ic = instanceConfig;
    String instanceName = ic.getInstanceName();
    String type;
    if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
      type = CommonConstants.Helix.SERVER_INSTANCE_TYPE;
    } else if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
      type = CommonConstants.Helix.BROKER_INSTANCE_TYPE;
    } else {
      throw new RuntimeException("Unknown instance type for: " + instanceName);
    }

    Instance instance = new Instance(ic.getHostName(),
        ic.getPort(),
        type, org.apache.commons.lang.StringUtils.join(ic.getTags(), ','));
    return instance;
  }

  @JsonCreator
  public Instance(
      @JsonProperty(value = "host", required = true) String host,
      @JsonProperty(value = "port", required = true) String port,
      @JsonProperty(value = "type", required = true) String type,
      @JsonProperty(value = "tag", required = false) String tag) {
    _host = host;
    _port = port;
    _tag = tag;

    if (CommonConstants.Helix.SERVER_INSTANCE_TYPE.equalsIgnoreCase(type)) {
      _instancePrefix = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE;
      _type = CommonConstants.Helix.SERVER_INSTANCE_TYPE;
    } else if (CommonConstants.Helix.BROKER_INSTANCE_TYPE.equalsIgnoreCase(type)) {
      _instancePrefix = CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE;
      _type = CommonConstants.Helix.BROKER_INSTANCE_TYPE;
    } else if (CommonConstants.Minion.INSTANCE_TYPE.equalsIgnoreCase(type)) {
      _instancePrefix = CommonConstants.Minion.INSTANCE_PREFIX;
      _type = CommonConstants.Minion.INSTANCE_TYPE;
    } else {
      throw new IllegalArgumentException("Invalid instance type " + type + ", expected either server or broker");
    }
  }

  public String getHost() {
    return _host;
  }

  public String getPort() {
    return _port;
  }

  public String getTag() {
    return _tag;
  }

  public String getType() {
    return _type;
  }

  public String toInstanceId() {
    return _instancePrefix + _host + "_" + _port;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("host : " + _host + "\n");
    bld.append("port : " + _port + "\n");
    bld.append("type : " + _type + "\n");
    if (_tag != null) {
      bld.append("tag : " + _tag + "\n");
    }
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("host", _host);
    ret.put("port", _port);
    ret.put("type", _type);
    ret.put("tag", getTagOrDefaultTag());
    return ret;
  }

  public InstanceConfig toInstanceConfig() {
    final InstanceConfig iConfig = new InstanceConfig(toInstanceId());
    iConfig.setHostName(_host);
    iConfig.setPort(_port);
    iConfig.setInstanceEnabled(true);
    iConfig.addTag(getTagOrDefaultTag());
    return iConfig;
  }

  private String getTagOrDefaultTag() {
    if (_tag != null) {
      return _tag;
    } else {
      switch (_type) {
        case CommonConstants.Helix.SERVER_INSTANCE_TYPE:
          return CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;
        case CommonConstants.Helix.BROKER_INSTANCE_TYPE:
          return CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
        case CommonConstants.Minion.INSTANCE_TYPE:
          return CommonConstants.Minion.UNTAGGED_INSTANCE;
        default:
          throw new RuntimeException("Unknown instance type " + _type + ", was expecting either server or broker");
      }
    }
  }
}
