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

import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.StringUtil;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 30, 2014
 */

public class Instance {

  private final String instanceHost;
  private final String instancePort;
  private final String tag;

  @JsonCreator
  public Instance(@JsonProperty("host") String host, @JsonProperty("port") String port, @JsonProperty("tag") String tag) {
    instanceHost = host;
    instancePort = port;
    this.tag = tag;
  }

  public String getInstanceHost() {
    return instanceHost;
  }

  public String getInstancePort() {
    return instancePort;
  }

  public String getTag() {
    return tag;
  }

  public String toInstanceId() {
    return StringUtil.join("_", instanceHost, instancePort);
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("host : " + instanceHost + "\n");
    bld.append("port : " + instancePort + "\n");
    if (tag != null) {
      bld.append("tag : " + tag + "\n");
    }
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("host", instanceHost);
    ret.put("port", instancePort);
    if (tag != null) {
      ret.put("tag", tag);
    } else {
      ret.put("tag", CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    return ret;
  }

  public InstanceConfig toInstanceConfig() {
    final InstanceConfig iConfig = new InstanceConfig(toInstanceId());
    iConfig.setHostName(instanceHost);
    iConfig.setPort(instancePort);
    iConfig.setInstanceEnabled(true);
    if (tag != null) {
      iConfig.addTag(tag);
    } else {
      iConfig.addTag(CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    return iConfig;
  }
}
