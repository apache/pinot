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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class JsonLogTransformerConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Name of the field that should contain extra fields that are not part of the schema.")
  private final String _indexableExtrasField;

  @JsonPropertyDescription(
      "Like indexableExtrasField except it only contains fields with the suffix in unindexableFieldSuffix.")
  private final String _unindexableExtrasField;

  @JsonPropertyDescription("The suffix of fields that must be stored in unindexableExtrasField")
  private final String _unindexableFieldSuffix;

  @JsonPropertyDescription("Array of field paths to drop")
  private final Set<String> _fieldPathsToDrop;

  @JsonCreator
  public JsonLogTransformerConfig(@JsonProperty("indexableExtrasField") String indexableExtrasField,
      @JsonProperty("unindexableExtrasField") @Nullable String unindexableExtrasField,
      @JsonProperty("unindexableFieldSuffix") @Nullable String unindexableFieldSuffix,
      @JsonProperty("fieldPathsToDrop") @Nullable Set<String> fieldPathsToDrop) {
    Preconditions.checkArgument(indexableExtrasField != null, "indexableExtrasField must be set");
    if (null != unindexableExtrasField) {
      Preconditions.checkArgument(null != unindexableFieldSuffix,
          "unindexableExtrasSuffix must be set if unindexableExtrasField is set");
    }
    _indexableExtrasField = indexableExtrasField;
    _unindexableExtrasField = unindexableExtrasField;
    _unindexableFieldSuffix = unindexableFieldSuffix;
    _fieldPathsToDrop = fieldPathsToDrop;
  }

  public String getIndexableExtrasField() {
    return _indexableExtrasField;
  }

  @Nullable
  public String getUnindexableExtrasField() {
    return _unindexableExtrasField;
  }

  @Nullable
  public String getUnindexableFieldSuffix() {
    return _unindexableFieldSuffix;
  }

  @Nullable
  public Set<String> getFieldPathsToDrop() {
    return _fieldPathsToDrop;
  }
}
