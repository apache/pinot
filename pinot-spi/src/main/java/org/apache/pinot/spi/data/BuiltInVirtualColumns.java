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

import java.util.List;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;


/// Single source of truth for the shape (name, data type, single-value vs multi-value) of the built-in virtual columns.
///
/// The columns are declared here once because they are materialized in two independent places that must agree:
///
/// - the broker/controller side adds them to the table schema used for query planning, without a provider class
///   (see `TableCache#addBuiltInVirtualColumns`), and
/// - the server side adds them to the segment schema together with the provider that produces the values
///   (see `VirtualColumnProviderFactory#addBuiltInVirtualColumnsToSegmentSchema`).
///
/// If the two sides disagreed on a data type or on single-value vs multi-value, the broker would declare one type
/// while the server produced another. Both sides therefore build their field specs from [#DEFINITIONS].
public class BuiltInVirtualColumns {
  private BuiltInVirtualColumns() {
  }

  /// Shape of a single built-in virtual column. The provider class is intentionally not part of this definition: it
  /// lives in `pinot-segment-local` and is only known to the server side.
  public static class Definition {
    private final String _name;
    private final DataType _dataType;
    private final boolean _singleValue;

    private Definition(String name, DataType dataType, boolean singleValue) {
      _name = name;
      _dataType = dataType;
      _singleValue = singleValue;
    }

    public String getName() {
      return _name;
    }

    public DataType getDataType() {
      return _dataType;
    }

    public boolean isSingleValue() {
      return _singleValue;
    }

    /// Creates the field spec for this column without a virtual column provider. Callers that can resolve a provider
    /// should set it on the returned spec.
    public DimensionFieldSpec createFieldSpec() {
      return new DimensionFieldSpec(_name, _dataType, _singleValue);
    }
  }

  public static final List<Definition> DEFINITIONS = List.of(
      new Definition(BuiltInVirtualColumn.DOCID, DataType.INT, true),
      new Definition(BuiltInVirtualColumn.HOSTNAME, DataType.STRING, true),
      new Definition(BuiltInVirtualColumn.SEGMENTNAME, DataType.STRING, true),
      new Definition(BuiltInVirtualColumn.PARTITIONID, DataType.STRING, false),
      new Definition(BuiltInVirtualColumn.CREATIONTIME, DataType.LONG, true),
      new Definition(BuiltInVirtualColumn.STARTTIMEMS, DataType.LONG, true),
      new Definition(BuiltInVirtualColumn.ENDTIMEMS, DataType.LONG, true),
      new Definition(BuiltInVirtualColumn.TOTALDOCS, DataType.INT, true),
      new Definition(BuiltInVirtualColumn.SEGMENTCRC, DataType.STRING, true));

  /// Adds the built-in virtual columns to the given schema, without a virtual column provider.
  ///
  /// Existing columns are left untouched, so a user-defined column of the same name always wins.
  public static void addToSchema(Schema schema) {
    for (Definition definition : DEFINITIONS) {
      if (!schema.hasColumn(definition.getName())) {
        schema.addField(definition.createFieldSpec());
      }
    }
  }
}
