package com.linkedin.pinot.query.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Contains aggregation part of the Query
 */
public class AggregationInfo implements Serializable {

  //TODO: Check if this can be an enum
  private String _aggregationType;

  private Map<String, String> _params;

  public String getAggregationType() {
    return _aggregationType;
  }

  public Map<String, String> getParams() {
    return _params;
  }

  public AggregationInfo(String aggregationType, Map<String, String> params) {
    super();
    _aggregationType = aggregationType;
    _params = params;
  }


  public AggregationInfo() {
    super();
  }

  public void setAggregationType(String aggregationType) {
    _aggregationType = aggregationType;
  }

  public void setParams(Map<String, String> params) {
    _params = params;
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

  public static List<AggregationInfo> fromJson(JSONArray jsonArray) {
    List<AggregationInfo> aggregations = new ArrayList<AggregationInfo>();
    for (int i =0; i < jsonArray.length(); i++)
    {
      aggregations.add(fromJson(jsonArray.getJSONObject(i)));
    }
    return aggregations;
  }
}

