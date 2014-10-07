package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * BrokerResource is used for broker to take care which data resource to serve.
 * 
 * @author xiafu
 *
 */
public class BrokerResource {

  private final String resourceName;
  private final String timeColumnName;
  private final String timeType;

  @JsonCreator
  public BrokerResource(@JsonProperty("resourceName") String resourceName,
      @JsonProperty("timeColumnName") String timeColumnName, @JsonProperty("timeType") String timeType) {
    this.resourceName = resourceName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeType() {
    return timeType;
  }

  public static BrokerResource fromMap(Map<String, String> props) {
    return new BrokerResource(props.get("resourceName"), props.get("timeColumnName"), props.get("timeType"));
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("timeColumnName", timeColumnName);
    props.put("timeType", timeType);
    return props;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("resourceName : " + resourceName + "\n");
    bld.append("timeColumnName : " + timeColumnName + "\n");
    bld.append("timeType : " + timeType + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("resourceName", resourceName);
    ret.put("timeColumnName", timeColumnName);
    ret.put("timeType", timeType);
    return ret;
  }

  public static void main(String[] args) {
    final ObjectMapper mapper = new ObjectMapper();

  }
}
