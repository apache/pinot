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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TypeFactoryTest {
  private static final TypeSystem TYPE_SYSTEM = new TypeSystem() {
  };

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
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema, false);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    for (RelDataTypeField field : fieldList) {
      switch (field.getName()) {
        case "INT_COL":
          BasicSqlType intBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER);
          Assert.assertEquals(field.getType(), intBasicSqlType);
          checkPrecisionScale(field, intBasicSqlType);
          break;
        case "LONG_COL":
          BasicSqlType bigIntBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BIGINT);
          Assert.assertEquals(field.getType(), bigIntBasicSqlType);
          checkPrecisionScale(field, bigIntBasicSqlType);
          break;
        case "FLOAT_COL":
        case "DOUBLE_COL":
          BasicSqlType doubleBasicSqlType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DOUBLE);
          Assert.assertEquals(field.getType(), doubleBasicSqlType);
          checkPrecisionScale(field, doubleBasicSqlType);
          break;
        case "STRING_COL":
        case "JSON_COL":
          Assert.assertEquals(field.getType(), new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
          break;
        case "BYTES_COL":
          Assert.assertEquals(field.getType(), new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY));
          break;
        case "INT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER), false));
          break;
        case "LONG_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BIGINT), false));
          break;
        case "FLOAT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.REAL), false));
          break;
        case "DOUBLE_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DOUBLE), false));
          break;
        case "STRING_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR), false));
          break;
        case "BYTES_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY), false));
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
