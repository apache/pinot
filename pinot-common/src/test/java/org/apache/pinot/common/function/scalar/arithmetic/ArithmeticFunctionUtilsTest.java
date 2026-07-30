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
package org.apache.pinot.common.function.scalar.arithmetic;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ArithmeticFunctionUtils#normalizeNumericType}, pinning that unsigned integer types
 * (CALCITE-1466, reachable under BABEL as of Calcite 1.41+) normalize to the narrowest signed integral type so the
 * polymorphic arithmetic scalar functions stay integral instead of widening to DOUBLE.
 */
public class ArithmeticFunctionUtilsTest {
  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  public void testNormalizeNumericTypeForUnsignedIntegers() {
    // The supported unsigned integer types (Calcite 1.41+, CALCITE-1466) must normalize to the narrowest signed
    // integral type that preserves their range so arithmetic scalar functions (negate/least/greatest/moduloOrZero)
    // stay integral instead of widening to DOUBLE: UTINYINT/USMALLINT fit in INTEGER; UINTEGER needs BIGINT. (UBIGINT
    // is unsupported -- rejected during plan conversion -- so it is not normalized here.)
    Assert.assertEquals(normalize(SqlTypeName.UTINYINT), SqlTypeName.INTEGER);
    Assert.assertEquals(normalize(SqlTypeName.USMALLINT), SqlTypeName.INTEGER);
    Assert.assertEquals(normalize(SqlTypeName.UINTEGER), SqlTypeName.BIGINT);
  }

  @Test
  public void testNormalizeNumericTypeForSignedTypesUnchanged() {
    // Sanity check that the signed/floating arms are unaffected by the unsigned additions.
    Assert.assertEquals(normalize(SqlTypeName.TINYINT), SqlTypeName.INTEGER);
    Assert.assertEquals(normalize(SqlTypeName.INTEGER), SqlTypeName.INTEGER);
    Assert.assertEquals(normalize(SqlTypeName.BIGINT), SqlTypeName.BIGINT);
    Assert.assertEquals(normalize(SqlTypeName.DECIMAL), SqlTypeName.DECIMAL);
    Assert.assertEquals(normalize(SqlTypeName.DOUBLE), SqlTypeName.DOUBLE);
  }

  private static SqlTypeName normalize(SqlTypeName sqlTypeName) {
    return ArithmeticFunctionUtils.normalizeNumericType(TYPE_FACTORY.createSqlType(sqlTypeName));
  }
}
