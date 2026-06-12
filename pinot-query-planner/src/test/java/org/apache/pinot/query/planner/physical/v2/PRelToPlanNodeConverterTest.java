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
package org.apache.pinot.query.planner.physical.v2;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PRelToPlanNodeConverter#convertToColumnDataType}. The v2 physical-optimizer converter
 * intentionally shares the same {@code SqlTypeName -> ColumnDataType} mapping as the v1
 * {@link org.apache.pinot.query.planner.logical.RelToPlanNodeConverter}; this test mirrors the unsigned-type coverage
 * in {@code RelToPlanNodeConverterTest} so the two converters cannot silently drift out of sync.
 */
public class PRelToPlanNodeConverterTest {

  @Test
  public void testConvertUnsignedIntegerTypes() {
    // Calcite 1.41+ (CALCITE-1466): the representable unsigned integer types map to the narrowest signed type that
    // holds their range (UTINYINT/USMALLINT -> INT, UINTEGER -> LONG); UBIGINT is rejected because no signed type
    // holds its full 0..2^64-1 range.
    Assert.assertEquals(PRelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.UTINYINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(PRelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.USMALLINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(PRelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.UINTEGER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertThrows(IllegalArgumentException.class, () -> PRelToPlanNodeConverter.convertToColumnDataType(
        new ObjectSqlType(SqlTypeName.UBIGINT, SqlIdentifier.STAR, true, null, null)));
  }

  @Test
  public void testConvertUnsignedIntegerArrayTypes() {
    Assert.assertEquals(PRelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.UINTEGER, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.LONG_ARRAY);
    Assert.assertThrows(IllegalArgumentException.class, () -> PRelToPlanNodeConverter.convertToColumnDataType(
        new ArraySqlType(new ObjectSqlType(SqlTypeName.UBIGINT, SqlIdentifier.STAR, true, null, null), true)));
  }
}
