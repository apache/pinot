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


/**
 * Maps SQL data type names to Pinot {@link DataType} values. Recognizes both standard SQL names
 * (BIGINT, VARCHAR, etc.) and Pinot-native aliases (LONG, STRING, BIG_DECIMAL, BYTES) that the
 * Calcite grammar already exposes via {@code config.fmpp}.
 */
public final class DataTypeMapper {
  private static final Map<String, DataType> NAME_TO_DATATYPE;

  static {
    Map<String, DataType> map = new HashMap<>();
    map.put("INT", DataType.INT);
    map.put("INTEGER", DataType.INT);
    map.put("SMALLINT", DataType.INT);
    map.put("TINYINT", DataType.INT);
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

  /**
   * Resolves a SQL type name (case-insensitive) to a Pinot {@link DataType}.
   *
   * @throws DdlCompilationException if the type is not supported.
   */
  public static DataType resolve(String sqlTypeName) {
    // Locale.ROOT: in Turkish locale "int".toUpperCase() yields "İNT" which fails the lookup.
    String upper = sqlTypeName.toUpperCase(Locale.ROOT);
    DataType dt = NAME_TO_DATATYPE.get(upper);
    if (dt == null) {
      throw new DdlCompilationException("Unsupported column data type: " + sqlTypeName);
    }
    return dt;
  }
}
