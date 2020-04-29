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
package org.apache.pinot.core.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Class to hold Theta Sketch Params, and is Json serializable.
 */
@SuppressWarnings("unused")
public class ThetaSketchParams {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String PARAMS_DELIMITER = ";";
  private static final String PARAM_KEY_VALUE_SEPARATOR = "=";

  @JsonProperty("nominalEntries")
  private int _nominalEntries;

  public int getNominalEntries() {
    return _nominalEntries;
  }

  public void setNominalEntries(int nominalEntries) {
    _nominalEntries = nominalEntries;
  }

  /**
   * Creates a ThetaSketchParams object from the specified string. The specified string is
   * expected to be of form: "key1 = value1; key2 = value2.."
   * @param paramsString Param in string form
   * @return Param object, null if string is null or empty
   */
  public static ThetaSketchParams fromString(String paramsString) {
    if (paramsString == null || paramsString.isEmpty()) {
      return null;
    }

    ObjectNode objectNode = JsonUtils.newObjectNode();
    for (String pair : paramsString.split(PARAMS_DELIMITER)) {
      String[] keyValue = pair.split(PARAM_KEY_VALUE_SEPARATOR);
      objectNode.put(keyValue[0].replaceAll("\\s", ""), keyValue[1].replaceAll("\\s", ""));
    }

    return OBJECT_MAPPER.convertValue(objectNode, ThetaSketchParams.class);
  }
}