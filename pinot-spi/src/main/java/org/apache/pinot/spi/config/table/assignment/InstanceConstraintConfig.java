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
package org.apache.pinot.spi.config.table.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class InstanceConstraintConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Name of the instance constraints to be applied (mandatory)")
  private final List<String> _constraints;

  @JsonCreator
  public InstanceConstraintConfig(@JsonProperty(value = "constraints", required = true) List<String> constraints) {
    Preconditions.checkArgument(constraints != null, "'constraints' must be configured");
    _constraints = constraints;
  }

  public List<String> getConstraints() {
    return _constraints;
  }
}
