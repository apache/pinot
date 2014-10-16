package com.linkedin.pinot.controller.api.pojos;

import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


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
      ret.put("tag", V1Constants.Helix.UNTAGGED_SERVER_INSTANCE);
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
      iConfig.addTag(V1Constants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    return iConfig;
  }
}
