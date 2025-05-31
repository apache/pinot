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
import java.util.List;
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Defines how configuration settings are propagated across workloads.
 * <p>
 * A PropagationScheme determines the scope and specific values (e.g., tables or tenants)
 * to which workload settings should be applied. This allows selective cascading
 * of resource and query limits across different instances.
 * </p>
 *
 * @see QueryWorkloadConfig
 * @see NodeConfig
 */
public class PropagationScheme extends BaseJsonConfig {

  /**
   * Enumerates the propagation scheme types that control the scope of propagation.
   * <p>
   * - TABLE: Propagate settings at the per-table level.<br>
   * - TENANT: Propagate settings at the tenant (logical group) level.
   * </p>
   */
  public enum Type {
    /** Propagate workload settings to individual tables. */
    TABLE("table"),
    /** Propagate workload settings to all tables under a tenant. */
    TENANT("tenant");

    private final String _value;

    Type(String value) {
      _value = value;
    }

    /**
     * Returns the JSON string representation of this propagation type.
     *
     * @return the JSON value corresponding to this Type (e.g., "table", "tenant")
     */
    @JsonValue
    public String getJsonValue() {
      return _value;
    }

    /**
     * Parses a JSON string into the corresponding Type enum.
     * <p>
     * Accepts case-insensitive and trimmed input matching defined JSON values.
     * </p>
     *
     * @param value JSON string to parse (may be null)
     * @return the matching Type enum, or null if input is null
     * @throws IllegalArgumentException if the input does not match any Type
     */
    @JsonCreator
    public static Type forValue(String value) {
      if (value == null) {
        return null;
      }
      String normalized = value.toLowerCase().trim();
      for (Type type : Type.values()) {
        if (type.getJsonValue().equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Invalid propagation scheme type: " + value);
    }
  }

  private static final String PROPAGATION_TYPE = "propagationType";
  private static final String VALUES = "values";

  /**
   * The type of propagation to apply (per-table or per-tenant).
   */
  @JsonPropertyDescription("Describes the type of propagation scheme")
  private Type _propagationType;

  /**
   * The specific identifiers (table names or tenant names) to which settings apply.
   */
  @JsonPropertyDescription("Describes the values of the propagation scheme")
  private List<String> _values;

  /**
   * Constructs a PropagationScheme with the given type and target values.
   *
   * @param propagationType the Type of propagation (TABLE or TENANT)
   * @param values the list of identifiers (tables or tenants) for propagation
   */
  @JsonCreator
  public PropagationScheme(@JsonProperty(PROPAGATION_TYPE) Type propagationType,
      @JsonProperty(VALUES) List<String> values) {
    _propagationType = propagationType;
    _values = values;
  }

  /**
   * Returns the configured propagation type.
   *
   * @return the Type enum indicating propagation scope
   */
  public Type getPropagationType() {
    return _propagationType;
  }

  /**
   * Returns the list of target identifiers for propagation.
   *
   * @return list of table names or tenant names
   */
  public List<String> getValues() {
    return _values;
  }

  /**
   * Sets the propagation type.
   *
   * @param propagationType new Type to define propagation scope
   */
  public void setPropagationType(Type propagationType) {
    _propagationType = propagationType;
  }

  /**
   * Sets the target identifiers for propagation.
   *
   * @param values list of table or tenant names to apply settings to
   */
  public void setValues(List<String> values) {
    _values = values;
  }
}
