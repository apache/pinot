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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;


/**
 * The {@code TypeSystem} overwrites Calcite type system with Pinot specific logics.
 */
public class TypeSystem extends RelDataTypeSystemImpl {
  private static final int MAX_DECIMAL_SCALE = 1000;
  private static final int MAX_DECIMAL_PRECISION = 1000;

  /**
   * Default precision for derived arithmetic decimal types(plus/multiply/divide/mod). We won't allow the return
   * precision to be larger than this value majorly due to the following reasons:
   * <ul><li>1. Precision computation is very costly, it should be explicitly specified</li>
   * <li>2. Work around the type hoist issue when doing de-correlation decimal type mismatch. See:
   * <a href="https://github.com/apache/pinot/pull/11151">Derive SUM return type to be PostgreSQL compatible</a>
   * for more details
   * </li></ul>
   */
  private static final int DERIVED_DECIMAL_PRECISION = 19;
  private static final int DERIVED_DECIMAL_SCALE = 1;

  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    // A "ragged" union refers to a union of two or more data types that don't all
    // have the same precision or scale. In these cases, Calcite may need to promote
    // one or more of the data types in order to maintain consistency.
    //
    // Pinot doesn't properly handle CHAR(FIXED) - by default, Calcite will cast a
    // CHAR(2) to a CHAR(3), but this will cause 2-char strings to be expanded with
    // spaces at the end (e.g. 'No' -> 'No '), which ultimately causes incorrect
    // behavior. This calcite flag will cause this to be cast to VARCHAR instead
    return true;
  }

  @Override
  public int getMaxNumericScale() {
    return MAX_DECIMAL_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return MAX_DECIMAL_PRECISION;
  }

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    assert SqlTypeUtil.isNumeric(argumentType);

    switch (argumentType.getSqlTypeName()) {
      case DECIMAL: {
        // For BIG_DECIMAL, set the return type to BIG_DECIMAL. Check OSS issue #10318 for more details.
        return argumentType;
      }
      default: {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
      }
    }
  }

  @Override
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    assert SqlTypeUtil.isNumeric(argumentType);
    switch (argumentType.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
            argumentType.isNullable());
      default:
        return argumentType;
    }
  }

  @Override
  public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    RelDataType dataType = super.deriveDecimalPlusType(typeFactory, type1, type2);
    if (dataType != null && SqlTypeUtil.isExactNumeric(dataType) && SqlTypeUtil.isDecimal(dataType)
        && (dataType.getPrecision() > DERIVED_DECIMAL_PRECISION)) {
      return typeFactory.createSqlType(SqlTypeName.DECIMAL, DERIVED_DECIMAL_PRECISION, DERIVED_DECIMAL_SCALE);
    }
    return dataType;
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    RelDataType dataType = super.deriveDecimalMultiplyType(typeFactory, type1, type2);
    if (dataType != null && SqlTypeUtil.isExactNumeric(dataType) && SqlTypeUtil.isDecimal(dataType)
        && (dataType.getPrecision() > DERIVED_DECIMAL_PRECISION)) {
      return typeFactory.createSqlType(SqlTypeName.DECIMAL, DERIVED_DECIMAL_PRECISION, DERIVED_DECIMAL_SCALE);
    }
    return dataType;
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    RelDataType dataType = super.deriveDecimalDivideType(typeFactory, type1, type2);
    if (dataType != null && SqlTypeUtil.isExactNumeric(dataType) && SqlTypeUtil.isDecimal(dataType)
        && (dataType.getPrecision() > DERIVED_DECIMAL_PRECISION)) {
      return typeFactory.createSqlType(SqlTypeName.DECIMAL, DERIVED_DECIMAL_PRECISION, DERIVED_DECIMAL_SCALE);
    }
    return dataType;
  }

  @Override
  public RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    RelDataType dataType = super.deriveDecimalModType(typeFactory, type1, type2);
    if (dataType != null && SqlTypeUtil.isExactNumeric(dataType) && SqlTypeUtil.isDecimal(dataType)
        && (dataType.getPrecision() > DERIVED_DECIMAL_PRECISION)) {
      return typeFactory.createSqlType(SqlTypeName.DECIMAL, DERIVED_DECIMAL_PRECISION, DERIVED_DECIMAL_SCALE);
    }
    return dataType;
  }
}
