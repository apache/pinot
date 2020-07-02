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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


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
  public UpsertConfig(@JsonProperty(value = "primaryKeyColumns") List<String> primaryKeyColumns,
      @JsonProperty(value = "offsetColumn") String offsetColumn,
      @JsonProperty(value = "validFromColumn") String validFromColumn,
      @JsonProperty(value = "validUntilColumn") String validUntilColumn) {
    Preconditions.checkArgument(primaryKeyColumns != null && primaryKeyColumns.size() == 1,
        "'primaryKeyColumns' must be configured with exact one column");
    Preconditions.checkArgument(StringUtils.isNotEmpty(offsetColumn), "'offsetColumn' must be configured");
    Preconditions.checkArgument(StringUtils.isNotEmpty(validFromColumn), "'validFromColumn' must be configured");
    Preconditions.checkArgument(StringUtils.isNotEmpty(validUntilColumn), "'validUntilColumn' must be configured");
    _primaryKeyColumns = primaryKeyColumns;
    _offsetColumn = offsetColumn;
    _validFromColumn = validFromColumn;
    _validUntilColumn = validUntilColumn;
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
