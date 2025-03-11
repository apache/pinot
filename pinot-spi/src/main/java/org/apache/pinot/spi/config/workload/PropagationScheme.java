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

public class PropagationScheme extends BaseJsonConfig {

  public enum Type {
    TABLE("table"),
    TENANT("tenant");

    private final String _value;

    Type(String value) {
      _value = value;
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

  @JsonPropertyDescription("Describes the type of propagation scheme")
  private Type _propagationType;

  @JsonPropertyDescription("Describes the values of the propagation scheme")
  private List<String> _values;

  @JsonCreator
  public PropagationScheme(@JsonProperty(PROPAGATION_TYPE) Type propagationType,
      @JsonProperty(VALUES) List<String> values) {
    _propagationType = propagationType;
    _values = values;
  }

  public Type getPropagationType() {
    return _propagationType;
  }

  public List<String> getValues() {
    return _values;
  }

  public void setPropagationType(Type propagationType) {
    _propagationType = propagationType;
  }

  public void setValues(List<String> values) {
    _values = values;
  }
}
