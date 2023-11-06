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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TypeFactoryTest {
  private static final TypeSystem TYPE_SYSTEM = new TypeSystem();

  @DataProvider(name = "relDataTypeConversion")
  public Iterator<Object[]> relDataTypeConversion() {
    ArrayList<Object[]> cases = new ArrayList<>();

    JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(TYPE_SYSTEM);

    for (FieldSpec.DataType dataType : FieldSpec.DataType.values()) {
      switch (dataType) {
        case INT: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.INTEGER);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case LONG: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.BIGINT);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        // Map float and double to the same RelDataType so that queries like
        // `select count(*) from table where aFloatColumn = 0.05` works correctly in multi-stage query engine.
        //
        // If float and double are mapped to different RelDataType,
        // `select count(*) from table where aFloatColumn = 0.05` will be converted to
        // `select count(*) from table where CAST(aFloatColumn as "DOUBLE") = 0.05`. While casting
        // from float to double does not always produce the same double value as the original float value, this leads to
        // wrong query result.
        //
        // With float and double mapped to the same RelDataType, the behavior in multi-stage query engine will be the
        // same as the query in v1 query engine.
        case FLOAT: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.DOUBLE);
          RelDataType arrayType = javaTypeFactory.createSqlType(SqlTypeName.REAL);
          cases.add(new Object[]{dataType, basicType, arrayType});
          break;
        }
        case DOUBLE: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.DOUBLE);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case BOOLEAN: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case TIMESTAMP: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case STRING: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.VARCHAR);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case BYTES: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.VARBINARY);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case BIG_DECIMAL: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.DECIMAL);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case JSON: {
          RelDataType basicType = javaTypeFactory.createSqlType(SqlTypeName.VARCHAR);
          cases.add(new Object[]{dataType, basicType, basicType});
          break;
        }
        case LIST:
        case STRUCT:
        case MAP:
        case UNKNOWN:
          break;
        default:
          String message = String.format("Unsupported type: %s ", dataType);
          throw new UnsupportedOperationException(message);
      }
    }
    return cases.iterator();
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col", dataType)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);
    Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(scalarType, true));
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNullableScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> field.setNullable(true))
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    RelDataType expectedType = typeFactory.createTypeWithNullability(scalarType, true);

    Assert.assertEquals(field.getType(), expectedType);
  }


  @Test(dataProvider = "relDataTypeConversion")
  public void testNotNullableScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> field.setNullable(false))
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    Assert.assertEquals(field.getType(), scalarType);
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testArrayTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addMultiValueDimension("col", dataType)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    RelDataType expectedType = typeFactory.createTypeWithNullability(typeFactory.createArrayType(arrayType, -1), true);

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNullableArrayTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> {
          field.setNullable(true);
          field.setSingleValueField(false);
        })
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    RelDataType expectedType = typeFactory.createTypeWithNullability(
        typeFactory.createArrayType(arrayType, -1),
        true
    );

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNotNullableArrayTypes(FieldSpec.DataType dataType, RelDataType scalarType, RelDataType arrayType) {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> {
          field.setNullable(false);
          field.setSingleValueField(false);
        })
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    RelDataType expectedType = typeFactory.createArrayType(arrayType, -1);

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test
  public void testRelDataTypeConversion() {
    TypeFactory typeFactory = new TypeFactory(TYPE_SYSTEM);
    Schema testSchema = new Schema.SchemaBuilder().addSingleValueDimension("INT_COL", FieldSpec.DataType.INT)
        .addSingleValueDimension("LONG_COL", FieldSpec.DataType.LONG)
        .addSingleValueDimension("FLOAT_COL", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("DOUBLE_COL", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("STRING_COL", FieldSpec.DataType.STRING)
        .addSingleValueDimension("BYTES_COL", FieldSpec.DataType.BYTES)
        .addSingleValueDimension("JSON_COL", FieldSpec.DataType.JSON)
        .addMultiValueDimension("INT_ARRAY_COL", FieldSpec.DataType.INT)
        .addMultiValueDimension("LONG_ARRAY_COL", FieldSpec.DataType.LONG)
        .addMultiValueDimension("FLOAT_ARRAY_COL", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("DOUBLE_ARRAY_COL", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("STRING_ARRAY_COL", FieldSpec.DataType.STRING)
        .addMultiValueDimension("BYTES_ARRAY_COL", FieldSpec.DataType.BYTES)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    for (RelDataTypeField field : fieldList) {
      switch (field.getName()) {
        case "INT_COL":
          BasicSqlType intBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER);
          Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(intBasicSqlType, true));
          checkPrecisionScale(field, intBasicSqlType);
          break;
        case "LONG_COL":
          BasicSqlType bigIntBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BIGINT);
          Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(bigIntBasicSqlType, true));
          checkPrecisionScale(field, bigIntBasicSqlType);
          break;
        case "FLOAT_COL":
        case "DOUBLE_COL":
          BasicSqlType doubleBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DOUBLE);
          Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(doubleBasicSqlType, true));
          checkPrecisionScale(field, doubleBasicSqlType);
          break;
        case "STRING_COL":
        case "JSON_COL": {
          BasicSqlType basicType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR);
          Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(basicType, true));
          break;
        }
        case "BYTES_COL":
          Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(
              new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY), true));
          break;
        case "INT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER), true));
          break;
        case "LONG_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BIGINT), true));
          break;
        case "FLOAT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.REAL), true));
          break;
        case "DOUBLE_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DOUBLE), true));
          break;
        case "STRING_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR), true));
          break;
        case "BYTES_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY), true));
          break;
        default:
          Assert.fail("Unexpected column name: " + field.getName());
          break;
      }
    }
  }

  //tests precision and scale for numeric data type
  private void checkPrecisionScale(RelDataTypeField field, BasicSqlType basicSqlType) {
    Assert.assertEquals(field.getValue().getPrecision(), basicSqlType.getPrecision());
    Assert.assertEquals(field.getValue().getScale(), basicSqlType.getScale());
  }
}
