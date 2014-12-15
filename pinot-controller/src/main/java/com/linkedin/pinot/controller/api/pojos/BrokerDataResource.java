package com.linkedin.pinot.controller.api.pojos;

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

  public static String CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE = "broker.dataResource.";
  private final String resourceName;
  private final int numBrokerInstances;
  private final String tag;

  @JsonCreator
  public BrokerDataResource(@JsonProperty("resourceName") String resourceName,
      @JsonProperty("numBrokerInstances") int numBrokerInstances, @JsonProperty("tag") String tag) {
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
    return new BrokerDataResource(props.get("resourceName"), Integer.parseInt(props.get("numBrokerInstances")),
        props.get("tag"));
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("numBrokerInstances", numBrokerInstances + "");
    props.put("tag", tag);
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
    ret.put("resourceName", resourceName);
    ret.put("numBrokerInstances", numBrokerInstances);
    ret.put("tag", tag);
    return ret;
  }

  public static void main(String[] args) {
    final Map<String, String> configs = new HashMap<String, String>();
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1.resourceName", "r1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1.numBrokerInstances", "1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r1.tag", "tag0");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2.resourceName", "r2");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2.numBrokerInstances", "2");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r2.tag", "tag1");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3.resourceName", "r3");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3.numBrokerInstances", "3");
    configs.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + "r3.tag", "tag1");
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
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + ".resourceName", resourceName);
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + ".numBrokerInstances", numBrokerInstances + "");
    props.put(CONFIG_PREFIX_OF_BROKER_DATA_RESOURCE + resourceName + ".tag", tag);
    return props;
  }
}
