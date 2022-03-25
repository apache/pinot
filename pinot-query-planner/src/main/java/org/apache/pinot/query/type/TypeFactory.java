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
package org.apache.pinot.query.type;

import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Extends Java-base TypeFactory from Calcite.
 *
 * <p>{@link JavaTypeFactoryImpl} is used here because we are not overriding much of the TypeFactory methods
 * required by Calcite. We will start extending {@link SqlTypeFactoryImpl} or even {@link RelDataTypeFactory}
 * when necessary for Pinot to override such mechanism.
 *
 * <p>Noted that {@link JavaTypeFactoryImpl} is subject to change. Please pay extra attention to this class when
 * upgrading Calcite versions.
 */
public class TypeFactory extends JavaTypeFactoryImpl {
  private final RelDataTypeSystem _typeSystem;

  public TypeFactory(RelDataTypeSystem typeSystem) {
    _typeSystem = typeSystem;
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema) {
    Builder builder = new Builder(this);
    for (Map.Entry<String, FieldSpec> e : schema.getFieldSpecMap().entrySet()) {
      builder.add(e.getKey(), toRelDataType(e.getValue()));
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return createSqlType(SqlTypeName.FLOAT);
      case DOUBLE:
        return createSqlType(SqlTypeName.DOUBLE);
      case BOOLEAN:
        return createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return createSqlType(SqlTypeName.TIMESTAMP);
      case STRING:
        return createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        return createSqlType(SqlTypeName.VARBINARY);
      case JSON:
        // TODO: support JSON, JSON should be supported using a special RelDataType as it is not a simple String,
        // nor can it be easily parsed as a STRUCT.
      case LIST:
        // TODO: support LIST, MV column should go fall into this category.
      case STRUCT:
      case MAP:
      default:
        throw new UnsupportedOperationException("unsupported!");
    }
  }
}
