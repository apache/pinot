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
package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonValue;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class NodeConfig extends BaseJsonConfig {

  public enum Type {
    LEAF_NODE("leafNode"),
    NON_LEAF_NODE("nonLeafNode");

    private final String jsonValue;

    Type(String jsonValue) {
      this.jsonValue = jsonValue;
    }

    @JsonValue
    public String getJsonValue() {
      return jsonValue;
    }

    @JsonCreator
    public static Type forValue(String value) {
      if (value == null) {
        return null;
      }
      // Normalize the input to lower case and trim spaces
      String normalized = value.toLowerCase().trim();
      for (Type type : Type.values()) {
        if (type.jsonValue.toLowerCase().equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Invalid node type: " + value);
    }
  }

  private static final String ENFORCEMENT_PROFILE = "enforcementProfile";
  private static final String PROPAGATION_SCHEME = "propagationScheme";

  @JsonPropertyDescription("Describes the enforcement profile for the node")
  private EnforcementProfile _enforcementProfile;

  @JsonPropertyDescription("Describes the propagation scheme for the node")
  private PropagationScheme _propagationScheme;

  @JsonCreator
  public NodeConfig(
      @JsonProperty(ENFORCEMENT_PROFILE) EnforcementProfile enforcementProfile,
      @JsonProperty(PROPAGATION_SCHEME) @Nullable PropagationScheme propagationScheme) {
    _enforcementProfile = enforcementProfile;
    _propagationScheme = propagationScheme;
  }

  public EnforcementProfile getEnforcementProfile() {
    return _enforcementProfile;
  }

  public PropagationScheme getPropagationScheme() {
    return _propagationScheme;
  }

  public void setEnforcementProfile(EnforcementProfile enforcementProfile) {
    _enforcementProfile = enforcementProfile;
  }

  public void setPropagationScheme(PropagationScheme propagationScheme) {
    _propagationScheme = propagationScheme;
  }
}
