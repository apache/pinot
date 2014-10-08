package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * BrokerTagResource is used for broker to take care of tag assignment.
 * 
 * @author xiafu
 *
 */
public class BrokerTagResource {

  public static String CONFIG_PREFIX_OF_BROKER_TAG = "broker.tag.";
  private final int numBrokerInstances;
  private final String tag;

  @JsonCreator
  public BrokerTagResource(@JsonProperty("numBrokerInstances") int numBrokerInstances, @JsonProperty("tag") String tag) {
    this.numBrokerInstances = numBrokerInstances;
    this.tag = tag;
  }

  public int getNumBrokerInstances() {
    return numBrokerInstances;
  }

  public String getTag() {
    return tag;
  }

  public static BrokerTagResource fromMap(Map<String, String> props) {
    return new BrokerTagResource(Integer.parseInt(props.get("numBrokerInstances")), props.get("tag"));
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("numBrokerInstances", numBrokerInstances + "");
    props.put("tag", tag);
    return props;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("numBrokerInstances : " + numBrokerInstances + "\n");
    bld.append("tag : " + tag + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("numBrokerInstances", numBrokerInstances);
    ret.put("tag", tag);
    return ret;
  }

  public static void main(String[] args) {
    Map<String, String> configs = new HashMap<String, String>();
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag0.numBrokerInstances", "1");
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag0.tag", "tag0");
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag1.numBrokerInstances", "2");
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag1.tag", "tag1");
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag2.numBrokerInstances", "3");
    configs.put(CONFIG_PREFIX_OF_BROKER_TAG + "tag2.tag", "tag2");
    System.out.println(fromMap(configs, "tag0"));
    System.out.println(fromMap(configs, "tag1"));
    System.out.println(fromMap(configs, "tag2"));

    System.out.println(fromMap(configs, "tag0").toBrokerConfigs());
    System.out.println(fromMap(configs, "tag1").toBrokerConfigs());
    System.out.println(fromMap(configs, "tag2").toBrokerConfigs());

  }

  public static BrokerTagResource fromMap(Map<String, String> configs, String tag) {
    Map<String, String> resourceBrokerConfig = new HashMap<String, String>();
    for (String key : configs.keySet()) {
      if (key.startsWith(CONFIG_PREFIX_OF_BROKER_TAG + tag)) {
        resourceBrokerConfig.put(key.split(CONFIG_PREFIX_OF_BROKER_TAG + tag + ".", 2)[1], configs.get(key));
      }
    }
    return BrokerTagResource.fromMap(resourceBrokerConfig);
  }

  public Map<String, String> toBrokerConfigs() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CONFIG_PREFIX_OF_BROKER_TAG + tag + ".numBrokerInstances", numBrokerInstances + "");
    props.put(CONFIG_PREFIX_OF_BROKER_TAG + tag + ".tag", tag);
    return props;
  }
}
