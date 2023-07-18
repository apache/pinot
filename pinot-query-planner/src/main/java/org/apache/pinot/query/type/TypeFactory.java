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
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.INTEGER)
            : createMultisetType(createSqlType(SqlTypeName.INTEGER), -1);
      case LONG:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.BIGINT)
            : createMultisetType(createSqlType(SqlTypeName.BIGINT), -1);
      case FLOAT:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.REAL)
            : createMultisetType(createSqlType(SqlTypeName.REAL), -1);
      case DOUBLE:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.DOUBLE)
            : createMultisetType(createSqlType(SqlTypeName.DOUBLE), -1);
      case BOOLEAN:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.BOOLEAN)
            : createMultisetType(createSqlType(SqlTypeName.BOOLEAN), -1);
      case TIMESTAMP:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.TIMESTAMP)
            : createMultisetType(createSqlType(SqlTypeName.TIMESTAMP), -1);
      case STRING:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.VARCHAR)
            : createMultisetType(createSqlType(SqlTypeName.VARCHAR), -1);
      case BYTES:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.VARBINARY)
            : createMultisetType(createSqlType(SqlTypeName.VARBINARY), -1);
      case BIG_DECIMAL:
        return fieldSpec.isSingleValueField() ? createSqlType(SqlTypeName.DECIMAL)
            : createMultisetType(createSqlType(SqlTypeName.DECIMAL), -1);
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
