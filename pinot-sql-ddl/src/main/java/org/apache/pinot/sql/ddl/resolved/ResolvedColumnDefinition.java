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
package org.apache.pinot.sql.ddl.resolved;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Normalized column definition produced by the DDL compiler. Independent of the Calcite parse
/// tree so downstream stages (Schema generation, reverse compilation) do not depend on the parser.
///
/// Immutable; instances are safe to share across threads.
public final class ResolvedColumnDefinition {
  private final String _name;
  private final DataType _dataType;
  private final ColumnRole _role;
  private final boolean _singleValue;
  private final boolean _notNull;
  private final String _defaultValue;
  private final String _dateTimeFormat;
  private final String _dateTimeGranularity;

  public ResolvedColumnDefinition(String name, DataType dataType, ColumnRole role, boolean singleValue,
      boolean notNull, @Nullable String defaultValue, @Nullable String dateTimeFormat,
      @Nullable String dateTimeGranularity) {
    _name = name;
    _dataType = dataType;
    _role = role;
    _singleValue = singleValue;
    _notNull = notNull;
    _defaultValue = defaultValue;
    _dateTimeFormat = dateTimeFormat;
    _dateTimeGranularity = dateTimeGranularity;
  }

  public String getName() {
    return _name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public ColumnRole getRole() {
    return _role;
  }

  public boolean isSingleValue() {
    return _singleValue;
  }

  public boolean isNotNull() {
    return _notNull;
  }

  @Nullable
  public String getDefaultValue() {
    return _defaultValue;
  }

  @Nullable
  public String getDateTimeFormat() {
    return _dateTimeFormat;
  }

  @Nullable
  public String getDateTimeGranularity() {
    return _dateTimeGranularity;
  }
}
