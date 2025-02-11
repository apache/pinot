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
package org.apache.pinot.tsdb.spi.plan.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


/**
 * We have implemented a custom serialization/deserialization mechanism for time series plans. This allows users to
 * use Jackson to annotate their plan nodes as shown in {@link LeafTimeSeriesPlanNode}, which is used for
 * plan serde for broker/server communication.
 * TODO: There are limitations to this and we will change this soon. Issues:
 *   1. Pinot TS SPI is compiled in Pinot distribution and Jackson deps get shaded usually.
 *   2. The plugins have to shade the dependency in the exact same way, which is obviously error-prone and not ideal.
 */
@InterfaceStability.Evolving
public class TimeSeriesPlanSerde {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private TimeSeriesPlanSerde() {
  }

  public static String serialize(BaseTimeSeriesPlanNode planNode) {
    try {
      return OBJECT_MAPPER.writeValueAsString(planNode);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while serializing plan", e);
    }
  }

  public static BaseTimeSeriesPlanNode deserialize(String planString) {
    try {
      JsonNode jsonNode = OBJECT_MAPPER.readTree(planString);
      return create(jsonNode);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while deserializing plan", e);
    }
  }

  public static BaseTimeSeriesPlanNode create(JsonNode jsonNode)
      throws JsonProcessingException, ClassNotFoundException {
    JsonNode inputs = null;
    if (jsonNode instanceof ObjectNode) {
      // Remove inputs field to prevent Jackson from deserializing it. We will handle it manually.
      ObjectNode objectNode = (ObjectNode) jsonNode;
      if (objectNode.has("inputs")) {
        inputs = objectNode.get("inputs");
        objectNode.remove("inputs");
      }
      objectNode.putIfAbsent("inputs", OBJECT_MAPPER.createArrayNode());
    }
    BaseTimeSeriesPlanNode planNode = null;
    try {
      String klassName = jsonNode.get("klass").asText();
      Class<BaseTimeSeriesPlanNode> klass = (Class<BaseTimeSeriesPlanNode>) Class.forName(klassName);
      planNode = OBJECT_MAPPER.readValue(jsonNode.toString(), klass);
    } finally {
      if (planNode != null && inputs instanceof ArrayNode) {
        ArrayNode childArray = (ArrayNode) inputs;
        for (JsonNode childJsonNode : childArray) {
          planNode.addInputNode(create(childJsonNode));
        }
      }
    }
    return planNode;
  }
}
