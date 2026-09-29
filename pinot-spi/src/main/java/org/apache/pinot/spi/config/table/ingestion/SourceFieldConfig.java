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
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.PinotDataType;


/// Configures a data type fix for a single source (input) field during ingestion, applied before other transformers
/// consume the field. Useful when a source field arrives with a type that a downstream enricher or transform
/// expression does not expect (e.g. an epoch timestamp arriving as a `String`).
///
/// Each config maps a source field ([#getName()]) to the target [PinotDataType] ([#getDataType()]) it should be
/// converted to. The [#isPreComplexTypeTransform()] flag selects when the fix runs:
/// - `true`: before the complex type transformation (and the pre-complex-type enrichers), so the corrected value can
///   feed complex type flattening and pre-complex-type enrichment.
/// - `false` (default): after the complex type transformation, so flattened/unnested fields can be fixed before the
///   post-complex-type enrichers and the expression transformer run.
///
/// A source field may be configured at most once per phase.
public class SourceFieldConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Name of the source field to fix the data type for")
  private final String _name;

  @JsonPropertyDescription("Target data type (PinotDataType name, e.g. INT, LONG, STRING, LONG_ARRAY) to convert "
      + "the source field to")
  private final PinotDataType _dataType;

  @JsonPropertyDescription("Whether the data type conversion is applied before the complex type transformation")
  private final boolean _preComplexTypeTransform;

  @JsonCreator
  public SourceFieldConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "dataType", required = true) PinotDataType dataType,
      @JsonProperty("preComplexTypeTransform") boolean preComplexTypeTransform) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "'name' must be set in SourceFieldConfig");
    Preconditions.checkArgument(dataType != null, "'dataType' must be set in SourceFieldConfig for source field: %s",
        name);
    _name = name;
    _dataType = dataType;
    _preComplexTypeTransform = preComplexTypeTransform;
  }

  public String getName() {
    return _name;
  }

  public PinotDataType getDataType() {
    return _dataType;
  }

  public boolean isPreComplexTypeTransform() {
    return _preComplexTypeTransform;
  }
}
