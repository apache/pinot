/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.utils;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.TreeMap;


public class ExplainPlanUtils {

  private ExplainPlanUtils() {
  }

  public static String formatExplainPlan(JsonNode explainPlanJson) {
    Map<Integer, String> nodesById = new TreeMap<>();
    JsonNode rows = explainPlanJson.get("resultTable").get("rows");
    int[] parentMapping = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      int id = row.get(1).asInt();
      if (id > 0) {
        parentMapping[id] = row.get(2).asInt();
      }
      nodesById.put(id, row.get(0).asText());
    }
    int[] depths = new int[rows.size()];
    for (Map.Entry<Integer, String> pair : nodesById.entrySet()) {
      int depth = 0;
      int id = pair.getKey();
      int parentId = id;
      while (parentId > 0) {
        depth++;
        parentId = parentMapping[parentId];
      }
      if (id > 0) {
        depths[id] = depth;
      }
    }
    StringBuilder explainPlan = new StringBuilder();
    for (Map.Entry<Integer, String> pair : nodesById.entrySet()) {
      explainPlan.append('\n');
      int id = pair.getKey();
      for (int i = 0; id > 0 && i < depths[id]; i++) {
        explainPlan.append('\t');
      }
      explainPlan.append(pair.getValue());
    }
    return explainPlan.toString();
  }
}
