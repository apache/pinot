package com.linkedin.pinot.pql.parsers.utils;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.pql.parsers.utils.JSONUtil;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.pinot.pql.parsers.utils.JSONUtil.FastJSONObject;


public class PQLParserUtils {
  public static void decorateWithMapReduce(JSONObject jsonObj,
      java.util.List<Pair<String, String>> aggreagationFunctions, JSONObject groupBy, String functionName,
      JSONObject parameters) {
    try {
      if (aggreagationFunctions == null) {
        aggreagationFunctions = new ArrayList<Pair<String, String>>();
      }
      if (aggreagationFunctions.size() > 0) {
        JSONObject meta = jsonObj.optJSONObject("meta");
        if (meta != null) {
          JSONArray selectList = meta.optJSONArray("select_list");

          if (selectList == null || selectList.length() == 0) {
            meta.put("select_list", new JSONUtil.FastJSONArray().put("*"));
          }
        }
      }
      JSONArray array = new JSONUtil.FastJSONArray();
      if (groupBy == null) {
        for (Pair<String, String> pair : aggreagationFunctions) {
          JSONObject props = new JSONUtil.FastJSONObject();

          props.put("column", pair.getSecond());
          props.put("mapReduce", pair.getFirst());
          array.put(props);
        }
      } else {
        JSONArray columns = groupBy.optJSONArray("columns");
        if (aggreagationFunctions.size() > 0) {
          groupBy.put("columns", (JSONArray) null);
        }
        int countSum = 0;
        int top = groupBy.optInt("top");
        for (Pair<String, String> pair : aggreagationFunctions) {
          //we need to skip the optimization for sum group by, as it breaks restli functionality in Pinot
          /*if (columns.length() == 1 && "sum".equalsIgnoreCase(pair.getFirst()) && countSum == 0) {
            countSum++;

            JSONObject facetSpec = new FastJSONObject().put("expand", false)
                .put("minhit", 0)
                .put("max", top).put("properties", new  FastJSONObject().put("dimension", columns.get(0)).put("metric", pair.getSecond()));
            if (jsonObj.opt("facets") == null) {
              jsonObj.put("facets", new FastJSONObject());
            }
            jsonObj.getJSONObject("facets").put(SenseiFacetHandlerBuilder.SUM_GROUP_BY_FACET_NAME, facetSpec);
          } else*/if (columns.length() == 1 && "count".equalsIgnoreCase(pair.getFirst())) {
            JSONObject facetSpec = new FastJSONObject().put("expand", false).put("minhit", 0).put("max", top);
            if (jsonObj.opt("facets") == null) {
              jsonObj.put("facets", new FastJSONObject());
            }
            jsonObj.getJSONObject("facets").put(columns.getString(0), facetSpec);
          } else {
            JSONObject props = new JSONUtil.FastJSONObject();

            props.put("function", pair.getFirst());
            props.put("metric", pair.getSecond());

            props.put("columns", columns);
            props.put("mapReduce", "sensei.groupBy");
            props.put("top", top);
            array.put(props);
          }
        }
      }
      if (functionName != null) {
        if (parameters == null) {
          parameters = new JSONUtil.FastJSONObject();
        }

        parameters.put("mapReduce", functionName);
        array.put(parameters);
      }
      JSONObject mapReduce = new JSONUtil.FastJSONObject();
      if (array.length() == 0) {
        return;
      }
      if (array.length() == 1) {
        JSONObject props = array.getJSONObject(0);
        mapReduce.put("function", props.get("mapReduce"));
        mapReduce.put("parameters", props);
      } else {
        mapReduce.put("function", "sensei.composite");
        JSONObject props = new JSONUtil.FastJSONObject();
        props.put("array", array);
        mapReduce.put("parameters", props);
      }
      jsonObj.put("mapReduce", mapReduce);
      // we need to remove group by since it's in Map reduce
      //jsonObj.remove("groupBy");
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
