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
import java.util.function.Predicate;
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
 * required by Calcite. We will start extending {@link org.apache.calcite.sql.type.SqlTypeFactoryImpl} or even
 * {@link org.apache.calcite.rel.type.RelDataTypeFactory} when necessary for Pinot to override such mechanism.
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
    Predicate<FieldSpec> isNullable;
    if (schema.isEnableColumnBasedNullHandling()) {
      isNullable = FieldSpec::isNullable;
    } else {
      isNullable = fieldSpec -> false;
    }
    for (Map.Entry<String, FieldSpec> e : schema.getFieldSpecMap().entrySet()) {
      builder.add(e.getKey(), toRelDataType(e.getValue(), isNullable));
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec, Predicate<FieldSpec> isNullable) {
    RelDataType type = createSqlType(getSqlTypeName(fieldSpec));
    boolean isArray = !fieldSpec.isSingleValueField();
    if (isArray) {
      type = createArrayType(type, -1);
    }
    if (isNullable.test(fieldSpec)) {
      type = createTypeWithNullability(type, true);
    }
    return type;
  }

  private SqlTypeName getSqlTypeName(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return SqlTypeName.INTEGER;
      case LONG:
        return SqlTypeName.BIGINT;
      // Map float and double to the same RelDataType so that queries like
      // `select count(*) from table where aFloatColumn = 0.05` works correctly in multi-stage query engine.
      //
      // If float and double are mapped to different RelDataType,
      // `select count(*) from table where aFloatColumn = 0.05` will be converted to
      // `select count(*) from table where CAST(aFloatColumn as "DOUBLE") = 0.05`. While casting
      // from float to double does not always produce the same double value as the original float value, this leads to
      // wrong query result.
      //
      // With float and double mapped to the same RelDataType, the behavior in multi-stage query engine will be the same
      // as the query in v1 query engine.
      case FLOAT:
        if (fieldSpec.isSingleValueField()) {
          return SqlTypeName.DOUBLE;
        } else {
          // TODO: This may be wrong. The reason why we want to use DOUBLE in single value float may also apply here
          return SqlTypeName.REAL;
        }
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      case STRING:
        return SqlTypeName.VARCHAR;
      case BYTES:
        return SqlTypeName.VARBINARY;
      case BIG_DECIMAL:
        return SqlTypeName.DECIMAL;
      case JSON:
        return SqlTypeName.VARCHAR;
      case LIST:
        // TODO: support LIST, MV column should go fall into this category.
      case STRUCT:
      case MAP:
      default:
        String message = String.format("Unsupported type: %s ", fieldSpec.getDataType());
        throw new UnsupportedOperationException(message);
    }
  }
}
