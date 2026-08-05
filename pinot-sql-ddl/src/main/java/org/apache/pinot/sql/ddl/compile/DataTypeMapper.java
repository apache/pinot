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
package org.apache.pinot.sql.ddl.compile;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Maps SQL data type names to Pinot [DataType] values. Recognizes both standard SQL names
/// (BIGINT, VARCHAR, etc.) and Pinot-native aliases (LONG, STRING, BIG_DECIMAL, BYTES) that the
/// Calcite grammar already exposes via `config.fmpp`.
public final class DataTypeMapper {
  private static final Map<String, DataType> NAME_TO_DATATYPE;

  static {
    Map<String, DataType> map = new HashMap<>();
    map.put("INT", DataType.INT);
    map.put("INTEGER", DataType.INT);
    map.put("BIGINT", DataType.LONG);
    map.put("LONG", DataType.LONG);
    map.put("FLOAT", DataType.FLOAT);
    map.put("REAL", DataType.FLOAT);
    map.put("DOUBLE", DataType.DOUBLE);
    map.put("DECIMAL", DataType.BIG_DECIMAL);
    map.put("NUMERIC", DataType.BIG_DECIMAL);
    map.put("BIG_DECIMAL", DataType.BIG_DECIMAL);
    map.put("BOOLEAN", DataType.BOOLEAN);
    map.put("TIMESTAMP", DataType.TIMESTAMP);
    map.put("VARCHAR", DataType.STRING);
    map.put("CHAR", DataType.STRING);
    map.put("STRING", DataType.STRING);
    map.put("VARBINARY", DataType.BYTES);
    map.put("BINARY", DataType.BYTES);
    map.put("BYTES", DataType.BYTES);
    map.put("JSON", DataType.JSON);
    NAME_TO_DATATYPE = map;
  }

  private DataTypeMapper() {
  }

  /// Resolves a SQL type name (case-insensitive) to a Pinot [DataType].
  ///
  /// @throws DdlCompilationException if the type is not supported.
  public static DataType resolve(String sqlTypeName) {
    // Locale.ROOT: in Turkish locale "int".toUpperCase() yields "İNT" which fails the lookup.
    String upper = sqlTypeName.toUpperCase(Locale.ROOT);
    // Reject SMALLINT / TINYINT explicitly rather than silently widening to INT. If Pinot
    // ever adds sub-INT integer types (INT8/INT16), existing DDLs using SMALLINT/TINYINT
    // would otherwise be locked into the wider type permanently. A rejected type can later
    // become accepted; a silently-promoted type cannot be narrowed without breaking users.
    if ("SMALLINT".equals(upper) || "TINYINT".equals(upper)) {
      throw new DdlCompilationException("Pinot does not yet support sub-INT integer types ("
          + sqlTypeName + "); use INT instead. This restriction is intentional so that future "
          + "narrow-integer types can be added without breaking existing DDL.");
    }
    DataType dt = NAME_TO_DATATYPE.get(upper);
    if (dt == null) {
      throw new DdlCompilationException("Unsupported column data type: " + sqlTypeName);
    }
    return dt;
  }
}
