package com.linkedin.pinot.common.query.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Contains aggregation part of the Query
 */
public class AggregationInfo implements Serializable {

  //TODO: Check if this can be an enum
  private String aggregationType;

  private Map<String, String> params;

  public String getAggregationType() {
    return aggregationType;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public AggregationInfo(String aggregationType, Map<String, String> params) {
    super();
    this.aggregationType = aggregationType;
    this.params = params;
  }


  public AggregationInfo() {
    super();
  }

  public void setAggregationType(String aggregationType) {
    this.aggregationType = aggregationType;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public static AggregationInfo fromJson(JSONObject jsonObject) {

    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);
    try {
      return mapper.readValue(jsonObject.toString(), AggregationInfo.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<AggregationInfo> fromJson(JSONArray jsonArray) throws JSONException {
    List<AggregationInfo> aggregations = new ArrayList<AggregationInfo>();
    for (int i =0; i < jsonArray.length(); i++)
    {
      aggregations.add(fromJson(jsonArray.getJSONObject(i)));
    }
    return aggregations;
  }

  @Override
  public String toString() {
    return "AggregationInfo [_aggregationType=" + aggregationType + ", _params=" + params + "]";
  }


}

