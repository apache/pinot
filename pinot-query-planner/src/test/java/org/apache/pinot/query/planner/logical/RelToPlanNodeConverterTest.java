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
package org.apache.pinot.query.planner.logical;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RelToPlanNodeConverterTest {

  @Test
  public void testConvertToColumnDataTypeForObjectTypes() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.BOOLEAN, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.BOOLEAN);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.TINYINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.SMALLINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.INTEGER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.BIGINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.FLOAT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.FLOAT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.DOUBLE, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.DOUBLE);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.TIMESTAMP, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.TIMESTAMP);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.CHAR, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.VARCHAR, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.VARBINARY, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.BYTES);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.OTHER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.OBJECT);
  }

  @Test
  public void testBigDecimal() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 10)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 38)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 39)),
        DataSchema.ColumnDataType.BIG_DECIMAL);

    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 14, 10)),
        DataSchema.ColumnDataType.FLOAT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 30, 10)),
        DataSchema.ColumnDataType.DOUBLE);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 31, 10)),
        DataSchema.ColumnDataType.BIG_DECIMAL);
  }

  @Test
  public void testConvertToColumnDataTypeForArray() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.BOOLEAN, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.BOOLEAN_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.TINYINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.SMALLINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.INTEGER, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.BIGINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.LONG_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.FLOAT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.FLOAT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.DOUBLE, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.DOUBLE_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.TIMESTAMP, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.TIMESTAMP_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.CHAR, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.STRING_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.VARCHAR, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.STRING_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.VARBINARY, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.BYTES_ARRAY);
  }
}
