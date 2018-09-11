/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.util;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class JsonResponseUtil {
  static ObjectMapper MAPPER = new ObjectMapper();

  public static ObjectNode buildSuccessResponseJSON(String message) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "OK");
    rootNode.put("Message", message);
    return rootNode;
  }

  public static ObjectNode buildErrorResponseJSON(String message) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "ERROR");
    rootNode.put("Message", message);
    return rootNode;
  }

  public static ObjectNode buildResponseJSON(Object obj) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "OK");
    JsonNode node = MAPPER.convertValue(obj, JsonNode.class);
    rootNode.put("Record", node);
    return rootNode;
  }

  public static ObjectNode buildResponseJSON(List<? extends Object> list) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    ArrayNode resultArrayNode = MAPPER.createArrayNode();
    rootNode.put("Result", "OK");
    for (Object obj : list) {
      JsonNode node = MAPPER.convertValue(obj, JsonNode.class);
      resultArrayNode.add(node);
    }
    rootNode.put("Records", resultArrayNode);
    return rootNode;
  }
}
