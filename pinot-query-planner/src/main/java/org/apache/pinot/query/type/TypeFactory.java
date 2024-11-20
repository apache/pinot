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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.ComplexFieldSpec;
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

  public static final TypeFactory INSTANCE = new TypeFactory();

  public TypeFactory() {
    super(TypeSystem.INSTANCE);
  }

  @Override
  public Charset getDefaultCharset() {
    return StandardCharsets.UTF_8;
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema) {
    Builder builder = new Builder(this);
    boolean enableNullHandling = schema.isEnableColumnBasedNullHandling();
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      builder.add(entry.getKey(), toRelDataType(entry.getValue(), enableNullHandling));
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec, boolean enableNullHandling) {
    SqlTypeName sqlTypeName = getSqlTypeName(fieldSpec);
    RelDataType type;
    if (sqlTypeName == SqlTypeName.MAP) {
      ComplexFieldSpec.MapFieldSpec mapFieldSpec = ComplexFieldSpec.toMapFieldSpec((ComplexFieldSpec) fieldSpec);
      type = createMapType(createSqlType(getSqlTypeName(mapFieldSpec.getKeyFieldSpec())),
          createSqlType(getSqlTypeName(mapFieldSpec.getValueFieldSpec())));
    } else {
      type = createSqlType(sqlTypeName);
      if (!fieldSpec.isSingleValueField()) {
        type = createArrayType(type, -1);
      }
    }
    return enableNullHandling && fieldSpec.isNullable() ? createTypeWithNullability(type, true) : type;
  }

  private static SqlTypeName getSqlTypeName(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return SqlTypeName.INTEGER;
      case LONG:
        return SqlTypeName.BIGINT;
      case FLOAT:
        return SqlTypeName.REAL;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case TIMESTAMP:
        return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      case TIMESTAMP_NTZ:
        return SqlTypeName.TIMESTAMP;
      case DATE:
        return SqlTypeName.DATE;
      case TIME:
        return SqlTypeName.TIME;
      case STRING:
      case JSON:
        return SqlTypeName.VARCHAR;
      case BYTES:
        return SqlTypeName.VARBINARY;
      case BIG_DECIMAL:
        return SqlTypeName.DECIMAL;
      case MAP:
        return SqlTypeName.MAP;
      case LIST:
        // TODO: support LIST, MV column should go fall into this category.
      case STRUCT:
      default:
        String message = String.format("Unsupported type: %s ", fieldSpec.getDataType());
        throw new UnsupportedOperationException(message);
    }
  }
}
