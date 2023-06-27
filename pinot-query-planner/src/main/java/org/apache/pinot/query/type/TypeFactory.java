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

  public TypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema, boolean isNullSupportEnabled) {
    Builder builder = new Builder(this);
    for (Map.Entry<String, FieldSpec> e : schema.getFieldSpecMap().entrySet()) {
      builder.add(e.getKey(), toRelDataType(e.getValue(),
          isNullSupportEnabled || e.getValue().isNullableField()));
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec, boolean isNullSupportEnabled) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.INTEGER)
                : createArrayType(createSqlType(SqlTypeName.INTEGER), -1), isNullSupportEnabled);
      case LONG:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.BIGINT)
                : createArrayType(createSqlType(SqlTypeName.BIGINT), -1), isNullSupportEnabled);
      case FLOAT:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.REAL)
            : createArrayType(createSqlType(SqlTypeName.REAL), -1), isNullSupportEnabled);
      case DOUBLE:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.DOUBLE)
                : createArrayType(createSqlType(SqlTypeName.DOUBLE), -1), isNullSupportEnabled);
      case BOOLEAN:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.BOOLEAN)
                : createArrayType(createSqlType(SqlTypeName.BOOLEAN), -1), isNullSupportEnabled);
      case TIMESTAMP:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.TIMESTAMP)
                : createArrayType(createSqlType(SqlTypeName.TIMESTAMP), -1), isNullSupportEnabled);
      case STRING:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.VARCHAR)
                : createArrayType(createSqlType(SqlTypeName.VARCHAR), -1), isNullSupportEnabled);
      case BYTES:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.VARBINARY)
                : createArrayType(createSqlType(SqlTypeName.VARBINARY), -1), isNullSupportEnabled);
      case BIG_DECIMAL:
        return createTypeWithNullability(fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.DECIMAL)
                : createArrayType(createSqlType(SqlTypeName.DECIMAL), -1), isNullSupportEnabled);
      case JSON:
        return createSqlType(SqlTypeName.VARCHAR);
      case LIST:
        // TODO: support LIST, MV column should go fall into this category.
      case STRUCT:
      case MAP:
      default:
        String message = String.format("Unsupported type: %s ", fieldSpec.getDataType().toString());
        throw new UnsupportedOperationException(message);
    }
  }
}
