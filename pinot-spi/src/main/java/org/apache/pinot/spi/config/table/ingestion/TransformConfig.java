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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Configs needed for performing simple transformations on the column
 */
public class TransformConfig extends BaseJsonConfig {

  public enum ValidationMode {
    STRICT,   // No automatic type conversions allowed
    LENIENT,  // Allow safe type conversions (recommended)
    LEGACY    // Allow all existing conversions including STRING->numeric (default)
  }

  @JsonPropertyDescription("Column name")
  private final String _columnName;

  @JsonPropertyDescription("Transformation function string")
  private final String _transformFunction;

  @JsonPropertyDescription("Validation mode for type checking: STRICT, LENIENT, or LEGACY")
  private final ValidationMode _validationMode;

  @JsonCreator
  public TransformConfig(@JsonProperty("columnName") String columnName,
      @JsonProperty("transformFunction") String transformFunction,
      @JsonProperty("validationMode") ValidationMode validationMode) {
    _columnName = columnName;
    _transformFunction = transformFunction;
    _validationMode = validationMode != null ? validationMode : ValidationMode.LEGACY;
  }

  // Backward compatibility constructor
  public TransformConfig(String columnName, String transformFunction) {
    this(columnName, transformFunction, ValidationMode.LEGACY);
  }

  public String getColumnName() {
    return _columnName;
  }

  public String getTransformFunction() {
    return _transformFunction;
  }

  public ValidationMode getValidationMode() {
    return _validationMode;
  }
}
