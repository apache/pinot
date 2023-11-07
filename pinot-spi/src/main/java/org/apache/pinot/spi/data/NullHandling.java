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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Objects;
import org.apache.pinot.spi.config.table.IndexingConfig;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mode")
@JsonSubTypes({
    @JsonSubTypes.Type(value = NullHandling.TableBased.class, name = "table"),
    @JsonSubTypes.Type(value = NullHandling.ColumnBased.class, name = "column")
})
public abstract class NullHandling {

  public abstract boolean isNullable(FieldSpec spec);

  /**
   * Returns whether this NullHandling supports V2 or not.
   *
   * In order to support V2 a null handling must be able to decide whether a column is nullable or not given its
   * {@link FieldSpec}.
   *
   * This predicate is both used as query and ingestion time.
   *
   * <table>
   *   <thead>
   *     <tr>
   *       <th>Supports v2</th>
   *       <th>Ingestion</th>
   *       <th>Query</th>
   *     </tr>
   *   </thead>
   *   <tbody>
   *     <tr>
   *       <td>Yes</td>
   *       <td>
   *         Null value vectors will be created for all columns in the table if and only
   *         if {@link IndexingConfig#isNullHandlingEnabled()} (or equivalent method) is true.
   *       </td>
   *       <td>
   *         If this method returns true: Null value vector buffers stored in the segment for all columns in the table
   *         will be read if and only if {@link IndexingConfig#isNullHandlingEnabled()}} is true
   *       </td>
   *     </tr>
   *     <tr>
   *       <td>False</td>
   *       <td>
   *         Null value vectors will be created for a column if and only if {@link NullHandling#isNullable(FieldSpec)}
   *         returns true for that column
   *       </td>
   *       <td>
   *         Null value vector buffers stored in the segment for a column will be read if and only if
   *         {@link NullHandling#isNullable(FieldSpec)} returns true for that column
   *       </td>
   *     </tr>
   *   </tbody>
   * </table>
   */
  public abstract boolean supportsV2();

  /**
   * This null handling mode indicates that nullability is defined by {@link IndexingConfig#isNullHandlingEnabled()}.
   *
   * For compatibility reasons this is the default mode, but it acts as all or nothing and it is recommended to
   * migrate to {@link ColumnBased}, which is more versatile.
   */
  @JsonTypeName("table")
  public static class TableBased extends NullHandling {
    private static final TableBased INSTANCE = new TableBased();

    private TableBased() {
    }

    @JsonCreator
    public static TableBased getInstance() {
      return INSTANCE;
    }

    @Override
    public boolean isNullable(FieldSpec spec) {
      return true;
    }

    @Override
    public boolean supportsV2() {
      return false;
    }

    @Override
    public String toString() {
      return "{\"mode\": \"table\"}";
    }
  }

  /**
   * This null handling mode indicates that nullability is defined by {@link FieldSpec#isNullable()}.
   *
   * It is important to note that multi-stage engine supports null handling if and only if this mode is enabled for
   * all the schemas that participate in the query.
   */
  @JsonTypeName("column")
  public static class ColumnBased extends NullHandling {
    private boolean _defaultValue = false;

    @JsonCreator
    public ColumnBased() {
    }

    public ColumnBased(boolean defaultValue) {
      _defaultValue = defaultValue;
    }

    @JsonIgnore
    @Override
    public boolean supportsV2() {
      return true;
    }

    @JsonProperty("default")
    public boolean isDefaultValue() {
      return _defaultValue;
    }

    public void setDefaultValue(boolean defaultValue) {
      _defaultValue = defaultValue;
    }

    @Override
    public boolean isNullable(FieldSpec spec) {
      return spec.isNullable() == Boolean.TRUE || spec.isNullable() == null && _defaultValue;
    }

    @Override
    public String toString() {
      return "{\"mode\": \"column\", \"default\": " + _defaultValue + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnBased that = (ColumnBased) o;
      return _defaultValue == that._defaultValue;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_defaultValue);
    }
  }
}
