/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.pql.parsers.utils;

import com.linkedin.pinot.common.Utils;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.pql.parsers.utils.JSONUtil.FastJSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PQLParserUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PQLParserUtils.class);
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
      LOGGER.error("Caught exception", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  /**
   * Converts MySQL LIKE expression to java Regex
   * @param expr
   * @return
   */
  public static String convertLikeExpressionToJavaRegex(String expr) {
    //escape all special characters used in java regex
    expr = expr.replaceAll(".", "\\.");
    expr = expr.replaceAll("[", "\\[");
    expr = expr.replaceAll("]", "\\]");
    expr = expr.replaceAll("(", "\\(");
    expr = expr.replaceAll(")", "\\)");
    //replace non escaped LIKE characters (% and _) with equivalent java regex
    expr = expr.replaceAll("[^\\]_", ".");
    expr = expr.replaceAll("[^\\%]", ".*?");
    //replace escaped % and _ characters with % and _ since java regex dont have any special meaning for that
    expr = expr.replaceAll("\\%", "%");
    expr = expr.replaceAll("\\_", "_");
    return expr;
  }
}
