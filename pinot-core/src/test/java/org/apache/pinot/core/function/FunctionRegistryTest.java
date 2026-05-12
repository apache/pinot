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
package org.apache.pinot.core.function;

import java.math.BigDecimal;
import java.util.EnumSet;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.sql.FilterKind;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


// NOTE: Keep this test in pinot-core to include all built-in scalar functions.
public class FunctionRegistryTest {
  // TODO: Support these functions
  private static final EnumSet<TransformFunctionType> IGNORED_TRANSFORM_FUNCTION_TYPES = EnumSet.of(
      // Special placeholder functions without implementation
      TransformFunctionType.SCALAR,
      // Special functions that requires index
      TransformFunctionType.JSON_EXTRACT_INDEX, TransformFunctionType.MAP_VALUE,
      TransformFunctionType.LOOKUP, TransformFunctionType.TEXT_MATCH,
      // TODO: Support these functions
      TransformFunctionType.IN, TransformFunctionType.NOT_IN, TransformFunctionType.IS_TRUE,
      TransformFunctionType.IS_NOT_TRUE, TransformFunctionType.IS_FALSE, TransformFunctionType.IS_NOT_FALSE,
      TransformFunctionType.JSON_EXTRACT_SCALAR,
      TransformFunctionType.JSON_EXTRACT_KEY, TransformFunctionType.TIME_CONVERT,
      TransformFunctionType.DATE_TIME_CONVERT_WINDOW_HOP, TransformFunctionType.ARRAY_LENGTH,
      TransformFunctionType.ARRAY_AVERAGE, TransformFunctionType.ARRAY_MIN, TransformFunctionType.ARRAY_MAX,
      TransformFunctionType.ARRAY_SUM, TransformFunctionType.VALUE_IN, TransformFunctionType.IN_ID_SET,
      TransformFunctionType.GROOVY, TransformFunctionType.CLP_DECODE, TransformFunctionType.CLP_ENCODED_VARS_MATCH,
      TransformFunctionType.ST_POLYGON, TransformFunctionType.ST_AREA, TransformFunctionType.ITEM,
      TransformFunctionType.TIME_SERIES_BUCKET);
  private static final EnumSet<FilterKind> IGNORED_FILTER_KINDS = EnumSet.of(
      // Special filter functions without implementation
      FilterKind.TEXT_MATCH, FilterKind.JSON_MATCH, FilterKind.VECTOR_SIMILARITY, FilterKind.VECTOR_SIMILARITY_RADIUS,
      FilterKind.SEMANTIC_MATCH,
      // TODO: Support these functions
      FilterKind.RANGE, FilterKind.IN, FilterKind.NOT_IN);

  @Test
  public void testTransformAndFilterFunctionsRegistered() {
    for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
      if (IGNORED_TRANSFORM_FUNCTION_TYPES.contains(transformFunctionType)) {
        continue;
      }
      for (String name : transformFunctionType.getNames()) {
        assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize(name)),
            "Unable to find transform function signature for: " + name);
      }
    }
    for (FilterKind filterKind : FilterKind.values()) {
      if (IGNORED_FILTER_KINDS.contains(filterKind)) {
        continue;
      }
      assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize(filterKind.name())),
          "Unable to find filter function signature for: " + filterKind);
    }
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(long randomArg1, String randomArg2) {
    return null;
  }

  @Test
  public void testScalarFunctionNames() {
    assertNotNull(FunctionRegistry.lookupFunctionInfo("testfunc1", 2));
    assertNotNull(FunctionRegistry.lookupFunctionInfo("testfunc2", 2));
    assertNull(FunctionRegistry.lookupFunctionInfo("testscalarfunction", 2));
    assertNull(FunctionRegistry.lookupFunctionInfo("testfunc1", 1));
    assertNull(FunctionRegistry.lookupFunctionInfo("testfunc2", 1));
  }

  @Test
  public void testBitFunctionPolymorphism() {
    FunctionInfo intBitAnd =
        FunctionRegistry.lookupFunctionInfo("bitand", new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    assertNotNull(intBitAnd);
    assertEquals(intBitAnd.getMethod().getName(), "intBitAnd");
    assertEquals(intBitAnd.getMethod().getReturnType(), int.class);

    FunctionInfo longBitAnd =
        FunctionRegistry.lookupFunctionInfo("bitand", new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT});
    assertNotNull(longBitAnd);
    assertEquals(longBitAnd.getMethod().getName(), "longBitAnd");
    assertEquals(longBitAnd.getMethod().getReturnType(), long.class);

    FunctionInfo intBitMask =
        FunctionRegistry.lookupFunctionInfo("bitmask", new ColumnDataType[]{ColumnDataType.INT});
    assertNotNull(intBitMask);
    assertEquals(intBitMask.getMethod().getName(), "intBitMask");
    assertEquals(intBitMask.getMethod().getReturnType(), long.class);

    FunctionInfo longBitMask =
        FunctionRegistry.lookupFunctionInfo("bitmask", new ColumnDataType[]{ColumnDataType.LONG});
    assertNotNull(longBitMask);
    assertEquals(longBitMask.getMethod().getName(), "longBitMask");
    assertEquals(longBitMask.getMethod().getReturnType(), long.class);

    FunctionInfo intBitExtract = FunctionRegistry.lookupFunctionInfo("bitextract",
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    assertNotNull(intBitExtract);
    assertEquals(intBitExtract.getMethod().getName(), "intBitExtract");
    assertEquals(intBitExtract.getMethod().getReturnType(), int.class);

    FunctionInfo longBitExtract = FunctionRegistry.lookupFunctionInfo("bitextract",
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG});
    assertNotNull(longBitExtract);
    assertEquals(longBitExtract.getMethod().getName(), "longBitExtract");
    assertEquals(longBitExtract.getMethod().getReturnType(), int.class);

    // Arity-only lookup returns the LONG overload as a safe fallback
    FunctionInfo bitmaskByArity = FunctionRegistry.lookupFunctionInfo("bitmask", 1);
    assertNotNull(bitmaskByArity);
    assertEquals(bitmaskByArity.getMethod().getName(), "longBitMask");

    FunctionInfo bitandByArity = FunctionRegistry.lookupFunctionInfo("bitand", 2);
    assertNotNull(bitandByArity);
    assertEquals(bitandByArity.getMethod().getName(), "longBitAnd");

    FunctionInfo bitextractByArity = FunctionRegistry.lookupFunctionInfo("bitextract", 2);
    assertNotNull(bitextractByArity);
    assertEquals(bitextractByArity.getMethod().getName(), "longBitExtract");
  }

  @Test
  public void testArithmeticFunctionPolymorphism() {
    FunctionInfo intAbs = FunctionRegistry.lookupFunctionInfo("abs", new ColumnDataType[]{ColumnDataType.INT});
    assertNotNull(intAbs);
    assertEquals(intAbs.getMethod().getName(), "intAbs");
    assertEquals(intAbs.getMethod().getReturnType(), int.class);

    FunctionInfo longAbs = FunctionRegistry.lookupFunctionInfo("abs", new ColumnDataType[]{ColumnDataType.LONG});
    assertNotNull(longAbs);
    assertEquals(longAbs.getMethod().getName(), "longAbs");
    assertEquals(longAbs.getMethod().getReturnType(), long.class);

    FunctionInfo floatAbs = FunctionRegistry.lookupFunctionInfo("abs", new ColumnDataType[]{ColumnDataType.FLOAT});
    assertNotNull(floatAbs);
    assertEquals(floatAbs.getMethod().getName(), "floatAbs");
    assertEquals(floatAbs.getMethod().getReturnType(), float.class);

    FunctionInfo bigDecimalAbs =
        FunctionRegistry.lookupFunctionInfo("abs", new ColumnDataType[]{ColumnDataType.BIG_DECIMAL});
    assertNotNull(bigDecimalAbs);
    assertEquals(bigDecimalAbs.getMethod().getName(), "bigDecimalAbs");
    assertEquals(bigDecimalAbs.getMethod().getReturnType(), BigDecimal.class);

    FunctionInfo stringAbs = FunctionRegistry.lookupFunctionInfo("abs", new ColumnDataType[]{ColumnDataType.STRING});
    assertNotNull(stringAbs);
    assertEquals(stringAbs.getMethod().getName(), "doubleAbs");
    assertEquals(stringAbs.getMethod().getReturnType(), double.class);

    FunctionInfo absByArity = FunctionRegistry.lookupFunctionInfo("abs", 1);
    assertNotNull(absByArity);
    assertEquals(absByArity.getMethod().getName(), "doubleAbs");

    FunctionInfo intMod =
        FunctionRegistry.lookupFunctionInfo("mod", new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    assertNotNull(intMod);
    assertEquals(intMod.getMethod().getName(), "intMod");
    assertEquals(intMod.getMethod().getReturnType(), int.class);

    FunctionInfo longMod =
        FunctionRegistry.lookupFunctionInfo("mod", new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT});
    assertNotNull(longMod);
    assertEquals(longMod.getMethod().getName(), "longMod");
    assertEquals(longMod.getMethod().getReturnType(), long.class);

    FunctionInfo floatMod =
        FunctionRegistry.lookupFunctionInfo("mod", new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.INT});
    assertNotNull(floatMod);
    assertEquals(floatMod.getMethod().getName(), "floatMod");
    assertEquals(floatMod.getMethod().getReturnType(), float.class);

    FunctionInfo bigDecimalMod = FunctionRegistry.lookupFunctionInfo("mod",
        new ColumnDataType[]{ColumnDataType.BIG_DECIMAL, ColumnDataType.INT});
    assertNotNull(bigDecimalMod);
    assertEquals(bigDecimalMod.getMethod().getName(), "bigDecimalMod");
    assertEquals(bigDecimalMod.getMethod().getReturnType(), BigDecimal.class);

    FunctionInfo stringMod =
        FunctionRegistry.lookupFunctionInfo("mod", new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    assertNotNull(stringMod);
    assertEquals(stringMod.getMethod().getName(), "doubleMod");
    assertEquals(stringMod.getMethod().getReturnType(), double.class);

    FunctionInfo modByArity = FunctionRegistry.lookupFunctionInfo("mod", 2);
    assertNotNull(modByArity);
    assertEquals(modByArity.getMethod().getName(), "doubleMod");

    FunctionInfo negateFloat =
        FunctionRegistry.lookupFunctionInfo("negate", new ColumnDataType[]{ColumnDataType.FLOAT});
    assertNotNull(negateFloat);
    assertEquals(negateFloat.getMethod().getName(), "floatNegate");
    assertEquals(negateFloat.getMethod().getReturnType(), float.class);

    FunctionInfo leastBigDecimal = FunctionRegistry.lookupFunctionInfo("least",
        new ColumnDataType[]{ColumnDataType.BIG_DECIMAL, ColumnDataType.DOUBLE});
    assertNotNull(leastBigDecimal);
    assertEquals(leastBigDecimal.getMethod().getName(), "bigDecimalLeast");
    assertEquals(leastBigDecimal.getMethod().getReturnType(), BigDecimal.class);
  }
}
