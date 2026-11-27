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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// Extends Java-base TypeFactory from Calcite.
///
/// [JavaTypeFactoryImpl] is used here because we are not overriding much of the TypeFactory methods
/// required by Calcite. We will start extending [org.apache.calcite.sql.type.SqlTypeFactoryImpl] or even
/// [org.apache.calcite.rel.type.RelDataTypeFactory] when necessary for Pinot to override such mechanism.
///
/// Noted that [JavaTypeFactoryImpl] is subject to change. Please pay extra attention to this class when
/// upgrading Calcite versions.
public class TypeFactory extends JavaTypeFactoryImpl {
  /// Child types an OPEN_STRUCT key read cannot carry as a Calcite type; such a key reads as JSON text.
  private static final Set<FieldSpec.DataType> COMPLEX_TYPES = EnumSet.of(FieldSpec.DataType.MAP,
      FieldSpec.DataType.LIST, FieldSpec.DataType.STRUCT, FieldSpec.DataType.OPEN_STRUCT);

  public static final TypeFactory INSTANCE = new TypeFactory();

  public TypeFactory() {
    super(TypeSystem.INSTANCE);
  }

  @Override
  public Charset getDefaultCharset() {
    return StandardCharsets.UTF_8;
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema) {
    return createRelDataTypeFromSchema(schema, column -> false);
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema, Predicate<String> shouldExclude) {
    Builder builder = new Builder(this);
    boolean enableNullHandling = schema.isEnableColumnBasedNullHandling();
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      String columnName = entry.getKey();
      if (!shouldExclude.test(columnName)) {
        builder.add(columnName, toRelDataType(entry.getValue(), enableNullHandling));
      }
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec, boolean enableNullHandling) {
    SqlTypeName sqlTypeName = getSqlTypeName(fieldSpec);
    RelDataType type;
    if (fieldSpec.getDataType() == FieldSpec.DataType.OPEN_STRUCT) {
      type = createOpenStructType((ComplexFieldSpec) fieldSpec, enableNullHandling);
    } else if (sqlTypeName == SqlTypeName.MAP) {
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

  /// `MAP<VARCHAR, VARCHAR>` carrying the declared child types; see [OpenStructSqlType]. A declared key is always
  /// nullable when null handling is on, since any row may lack it. A complex child (MAP, LIST, STRUCT, OPEN_STRUCT)
  /// is VARCHAR: the single-stage engine reads it as JSON text.
  private RelDataType createOpenStructType(ComplexFieldSpec fieldSpec, boolean enableNullHandling) {
    Map<String, RelDataType> declaredKeyTypes = new HashMap<>();
    for (Map.Entry<String, FieldSpec> child : fieldSpec.getChildFieldSpecs().entrySet()) {
      FieldSpec childSpec = child.getValue();
      RelDataType childType;
      if (COMPLEX_TYPES.contains(childSpec.getDataType())) {
        childType = createSqlType(SqlTypeName.VARCHAR);
      } else {
        childType = createSqlType(getSqlTypeName(childSpec));
        if (!childSpec.isSingleValueField()) {
          childType = createArrayType(childType, -1);
        }
      }
      declaredKeyTypes.put(child.getKey(), createTypeWithNullability(childType, enableNullHandling));
    }
    RelDataType varchar = createSqlType(SqlTypeName.VARCHAR);
    return canonize(new OpenStructSqlType(varchar, varchar, false, declaredKeyTypes));
  }

  /// Keeps an [OpenStructSqlType] when its nullability changes; Calcite's copy would degrade it to a plain map.
  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    if (type instanceof OpenStructSqlType openStruct) {
      if (openStruct.isNullable() == nullable) {
        return type;
      }
      return canonize(new OpenStructSqlType(openStruct.getKeyType(), openStruct.getValueType(), nullable,
          openStruct.getDeclaredKeyTypes()));
    }
    return super.createTypeWithNullability(type, nullable);
  }

  /// Keeps an [OpenStructSqlType] when every input is one (for example the branches of a UNION ALL); Calcite's
  /// version would return a plain map, which the plan converters do not map to JSON. The result declares the keys
  /// every input declares with the same type (nullable if any input's is), and is nullable when any input is.
  ///
  /// A NULL input is not handled: Calcite casts it to the union type through a type spec, which loses the subclass.
  @Override
  @Nullable
  public RelDataType leastRestrictive(List<RelDataType> types) {
    Map<String, RelDataType> declaredKeyTypes = null;
    boolean nullable = false;
    for (RelDataType type : types) {
      if (!(type instanceof OpenStructSqlType openStruct)) {
        return super.leastRestrictive(types);
      }
      nullable |= openStruct.isNullable();
      if (declaredKeyTypes == null) {
        declaredKeyTypes = new HashMap<>(openStruct.getDeclaredKeyTypes());
        continue;
      }
      Iterator<Map.Entry<String, RelDataType>> iterator = declaredKeyTypes.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, RelDataType> entry = iterator.next();
        RelDataType other = openStruct.getDeclaredKeyType(entry.getKey());
        if (other == null || !SqlTypeUtil.equalSansNullability(this, entry.getValue(), other)) {
          iterator.remove();
        } else if (other.isNullable()) {
          entry.setValue(createTypeWithNullability(entry.getValue(), true));
        }
      }
    }
    if (declaredKeyTypes == null) {
      return super.leastRestrictive(types);
    }
    RelDataType varchar = createSqlType(SqlTypeName.VARCHAR);
    return canonize(new OpenStructSqlType(varchar, varchar, nullable, declaredKeyTypes));
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
        return SqlTypeName.TIMESTAMP;
      case STRING:
      case JSON:
        return SqlTypeName.VARCHAR;
      // See OpenStructSqlType. Without a mapping, building the row type fails for every query on the table.
      case OPEN_STRUCT:
        return SqlTypeName.MAP;
      case UUID:
        return SqlTypeName.UUID;
      case VARIANT:
        return SqlTypeName.VARIANT;
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
