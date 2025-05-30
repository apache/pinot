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

/**
 * Represents the configuration for a specific node type in a query workload.
 * <p>
 * Each NodeConfig specifies:
 * <ul>
 *   <li><strong>Node Type:</strong> The role of the node in processing queries.</li>
 *   <li><strong>Enforcement Profile:</strong> Resource limits (CPU and memory) applied to this node.</li>
 *   <li><strong>Propagation Scheme:</strong> Optional instructions for cascading configs to downstream nodes.</li>
 * </ul>
 * </p>
 * <p>
 * This class is used within {@link QueryWorkloadConfig} to define per-node settings
 * that tailor query execution behavior based on workload classification.
 * </p>
 *
 * @see QueryWorkloadConfig
 * @see EnforcementProfile
 * @see PropagationScheme
 */
public class NodeConfig extends BaseJsonConfig {

  public enum Type {
    BROKER_NODE("brokerNode"),
    SERVER_NODE("serverNode");

    private final String _value;

    Type(String jsonValue) {
      _value = jsonValue;
    }

    @JsonValue
    public String getJsonValue() {
      return _value;
    }

    @JsonCreator
    public static Type forValue(String value) {
      if (value == null) {
        return null;
      }
      // Normalize the input to lower case and trim spaces
      String normalized = value.toLowerCase().trim();
      for (Type type : Type.values()) {
        if (type.getJsonValue().toLowerCase().equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Invalid node type: " + value);
    }
  }

  private static final String NODE_TYPE = "nodeType";
  private static final String ENFORCEMENT_PROFILE = "enforcementProfile";
  private static final String PROPAGATION_SCHEME = "propagationScheme";

  /**
   * The role of this node within the query workload, indicating whether it directly serves
   * queries or acts as an intermediate forwarding node.
   */
  @JsonPropertyDescription("Describes the type of node")
  private Type _nodeType;

  /**
   * The resource enforcement profile for this node, defining limits on CPU and memory
   * usage for queries under this workload.
   */
  @JsonPropertyDescription("Describes the enforcement profile for the node")
  private EnforcementProfile _enforcementProfile;

  /**
   * Optional propagation scheme that specifies how configuration settings are cascaded
   * or shared with downstream nodes; may be null if no propagation is applied.
   */
  @JsonPropertyDescription("Describes the propagation scheme for the node")
  private PropagationScheme _propagationScheme;

  @JsonCreator
  public NodeConfig(
      @JsonProperty(NODE_TYPE) Type nodeType,
      @JsonProperty(ENFORCEMENT_PROFILE) EnforcementProfile enforcementProfile,
      @JsonProperty(PROPAGATION_SCHEME) @Nullable PropagationScheme propagationScheme) {
    _nodeType = nodeType;
    _enforcementProfile = enforcementProfile;
    _propagationScheme = propagationScheme;
  }

  public Type getNodeType() {
      return _nodeType;
  }

  public EnforcementProfile getEnforcementProfile() {
    return _enforcementProfile;
  }

  public PropagationScheme getPropagationScheme() {
    return _propagationScheme;
  }
}
