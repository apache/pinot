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
package org.apache.pinot.calcite.sql2rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PinotRelDecorrelator#verifyRowTypePreserved}, the replacement for Calcite's
 * {@code Litmus.THROW} post-decorrelation assertion (CALCITE-7379). The structural-divergence (throw) branch is the
 * safety net that distinguishes a tolerable nullability-only change from a real wrong-typed plan; it is not reachable
 * from a passing SQL query, so it is pinned here with hand-built row-type fixtures.
 */
public class PinotRelDecorrelatorTest {
  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private static RelDataType row(boolean nullable, SqlTypeName... fieldTypes) {
    RelDataTypeFactory.Builder builder = TYPE_FACTORY.builder();
    for (int i = 0; i < fieldTypes.length; i++) {
      builder.add("f" + i, TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(fieldTypes[i]), nullable));
    }
    return builder.build();
  }

  @Test
  public void testIdenticalRowTypePasses() {
    RelDataType type = row(false, SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    // Identical row types short-circuit before any divergence handling.
    PinotRelDecorrelator.verifyRowTypePreserved(type, row(false, SqlTypeName.INTEGER, SqlTypeName.VARCHAR));
  }

  @Test
  public void testNullabilityOnlyDivergenceTolerated() {
    // The CALCITE-7379 case: same structure, different nullability -> warn and continue, no throw.
    RelDataType before = row(false, SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    RelDataType after = row(true, SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    PinotRelDecorrelator.verifyRowTypePreserved(before, after);
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testStructuralBaseTypeDivergenceThrows() {
    // A genuine base-type change must fail fast rather than silently producing a wrong-typed plan.
    PinotRelDecorrelator.verifyRowTypePreserved(row(false, SqlTypeName.INTEGER), row(false, SqlTypeName.BIGINT));
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testStructuralFieldCountDivergenceThrows() {
    PinotRelDecorrelator.verifyRowTypePreserved(
        row(false, SqlTypeName.INTEGER), row(false, SqlTypeName.INTEGER, SqlTypeName.INTEGER));
  }
}
