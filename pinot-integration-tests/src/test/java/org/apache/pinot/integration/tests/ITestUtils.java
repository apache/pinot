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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.jetbrains.annotations.NotNull;


public class ITestUtils {

  static final String RESULT_TABLE = "resultTable";

  private ITestUtils() {
  }

  public static @NotNull String toExplainStr(JsonNode mainNode) {
    if (mainNode == null) {
      return "null";
    }
    JsonNode node = mainNode.get(RESULT_TABLE);
    if (node == null) {
      return ITestUtils.toErrorString(mainNode);
    }
    return toExplainString(node);
  }

  public static String toExplainString(JsonNode node) {
    return node.get("rows").get(0).get(1).textValue();
  }

  public static @NotNull String toResultStr(JsonNode mainNode) {
    if (mainNode == null) {
      return "null";
    }
    JsonNode node = mainNode.get(RESULT_TABLE);
    if (node == null) {
      return toErrorString(mainNode);
    }
    return toString(node);
  }

  public static String toErrorString(JsonNode topNode) {
    JsonNode node = topNode.get("exceptions");
    JsonNode jsonNode = node.get(0);
    if (jsonNode != null) {
      return jsonNode.get("message").textValue();
    }
    return "";
  }

  public static String toString(JsonNode node) {
    StringBuilder buf = new StringBuilder();
    ArrayNode columnNames = (ArrayNode) node.get("dataSchema").get("columnNames");
    ArrayNode columnTypes = (ArrayNode) node.get("dataSchema").get("columnDataTypes");
    ArrayNode rows = (ArrayNode) node.get("rows");

    for (int i = 0; i < columnNames.size(); i++) {
      JsonNode name = columnNames.get(i);
      JsonNode type = columnTypes.get(i);

      if (i > 0) {
        buf.append(",\t");
      }

      buf.append(name).append('[').append(type).append(']');
    }

    for (int i = 0; i < rows.size(); i++) {
      ArrayNode row = (ArrayNode) rows.get(i);

      buf.append('\n');
      for (int j = 0; j < row.size(); j++) {
        if (j > 0) {
          buf.append(",\t");
        }

        buf.append(row.get(j));
      }
    }

    return buf.toString();
  }
}
