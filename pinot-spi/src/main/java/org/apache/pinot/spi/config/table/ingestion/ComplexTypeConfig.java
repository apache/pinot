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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Config related to handling complex type
 */
public class ComplexTypeConfig extends BaseJsonConfig {

  public enum CollectionNotUnnestedToJson {
    NONE, NON_PRIMITIVE, ALL
  }

  @JsonPropertyDescription("The fields to unnest")
  private final List<String> _fieldsToUnnest;

  @JsonPropertyDescription("The delimiter used to separate components in a path")
  private final String _delimiter;

  @JsonPropertyDescription("The mode of converting collection to JSON string")
  private final CollectionNotUnnestedToJson _collectionNotUnnestedToJson;

  @JsonCreator
  public ComplexTypeConfig(@JsonProperty("fieldsToUnnest") @Nullable List<String> fieldsToUnnest,
      @JsonProperty("delimiter") @Nullable String delimiter,
      @JsonProperty("collectionNotUnnestedToJson") @Nullable CollectionNotUnnestedToJson collectionNotUnnestedToJson) {
    _fieldsToUnnest = fieldsToUnnest;
    _delimiter = delimiter;
    _collectionNotUnnestedToJson = collectionNotUnnestedToJson;
  }

  @Nullable
  public List<String> getFieldsToUnnest() {
    return _fieldsToUnnest;
  }

  @Nullable
  public String getDelimiter() {
    return _delimiter;
  }

  @Nullable
  public CollectionNotUnnestedToJson getCollectionNotUnnestedToJson() {
    return _collectionNotUnnestedToJson;
  }
}
