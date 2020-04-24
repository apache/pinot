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
package org.apache.pinot.spi.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.List;

@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpsertConfig extends BaseJsonConfig {
  // names of the columns that used as primary keys of an upsert table
  private final List<String> _primaryKeyColumns;
  // name of the column that we are going to store the offset value to
  private final String _offsetColumn;
  // name of the virtual column that we are going to store the validFrom value to
  private final String _validFromColumn;
  // name of the virtual column that we are going to store the validUntil value to
  private final String _validUntilColumn;

  @JsonCreator
  public UpsertConfig(
      @JsonProperty(value="primaryKeyColumns") List<String> primaryKeyColumns,
      @JsonProperty(value="offsetColumn") String offsetColumn,
      @JsonProperty(value="validFromColumn") String validFromColumn,
      @JsonProperty(value="validUntilColumn") String validUntilColumn) {

    _primaryKeyColumns = primaryKeyColumns;
    _offsetColumn = offsetColumn;
    _validFromColumn = validFromColumn;
    _validUntilColumn = validUntilColumn;

    Preconditions.checkState(_primaryKeyColumns.size() == 1,
        "Upsert feature supports only one primary key column");
    Preconditions.checkState(StringUtils.isNotEmpty(_offsetColumn),
        "upsert feature requires \"offsetColumn\" to be set ");
    Preconditions.checkState(StringUtils.isNotEmpty(_validFromColumn),
        "Upsert feature requires \"validFromColumn\" to be set");
    Preconditions.checkState(StringUtils.isNotEmpty(_validUntilColumn),
        "Upsert feature requires \"validUntilColumn\" to be set");
  }

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public String getOffsetColumn() {
    return _offsetColumn;
  }

  public String getValidFromColumn() {
    return _validFromColumn;
  }

  public String getValidUntilColumn() {
    return _validUntilColumn;
  }
}
