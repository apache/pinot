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
package org.apache.pinot.common.function.scalar;

import java.math.BigDecimal;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class ObjectFunctions {
  private ObjectFunctions() {
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNull(@Nullable Object obj) {
    return obj == null;
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNotNull(@Nullable Object obj) {
    return !isNull(obj);
  }

  /// Numbers are compared by value, so `5` and `5L` are not distinct. Arrays (e.g. `BYTES` values, which are `byte[]`)
  /// are compared by content. Other values are compared with [Object#equals].
  @ScalarFunction(nullableParameters = true)
  public static boolean isDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    if (obj1 == null && obj2 == null) {
      return false;
    }
    if (obj1 == null || obj2 == null) {
      return true;
    }
    if (isNumber(obj1) && isNumber(obj2)) {
      return !numberEquals((Number) obj1, (Number) obj2);
    }
    return !Objects.deepEquals(obj1, obj2);
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNotDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    return !isDistinctFrom(obj1, obj2);
  }

  private static boolean isNumber(Object obj) {
    return obj instanceof Integer || obj instanceof Long || obj instanceof Float || obj instanceof Double
        || obj instanceof BigDecimal;
  }

  private static boolean isFloatingPoint(Number number) {
    return number instanceof Float || number instanceof Double;
  }

  /// Compares two numbers by value, so that numbers of different types (e.g. `Integer` and `Long`) can be equal. This
  /// is close to `BinaryOperatorTransformFunction`, which runs `isDistinctFrom` in the single-stage engine and in the
  /// multi-stage leaf stage. It differs only in two edge cases: a `long` above 2^53 is compared exactly with a
  /// floating point number, and NaN or infinity compared with a `BigDecimal` does not throw.
  ///   - Two integers are compared as `long`.
  ///   - Two floating point numbers are compared with [Double#compare], like [Double#equals]: a `float` is widened
  ///     (`1.1f` does not equal `1.1`), NaN equals NaN, and `-0.0` does not equal `0.0`.
  ///   - An integer and a floating point number are compared exactly.
  ///   - When either number is a `BigDecimal`, both are compared as `BigDecimal`, which ignores the scale (`1.5`
  ///     equals `1.50`). A floating point number is converted with [BigDecimal#valueOf(double)], so `0.1` equals the
  ///     `BigDecimal` `0.1`. NaN and infinity do not equal any `BigDecimal`.
  private static boolean numberEquals(Number number1, Number number2) {
    if (number1 instanceof BigDecimal || number2 instanceof BigDecimal) {
      BigDecimal value1 = toBigDecimal(number1);
      BigDecimal value2 = toBigDecimal(number2);
      return value1 != null && value2 != null && value1.compareTo(value2) == 0;
    }
    boolean isFloatingPoint1 = isFloatingPoint(number1);
    boolean isFloatingPoint2 = isFloatingPoint(number2);
    if (!isFloatingPoint1 && !isFloatingPoint2) {
      return number1.longValue() == number2.longValue();
    }
    if (isFloatingPoint1 && isFloatingPoint2) {
      return Double.compare(number1.doubleValue(), number2.doubleValue()) == 0;
    }
    long longValue = isFloatingPoint1 ? number2.longValue() : number1.longValue();
    double doubleValue = isFloatingPoint1 ? number1.doubleValue() : number2.doubleValue();
    if (Math.abs(longValue) <= 1L << 53) {
      // The long converts to double exactly
      return Double.compare(longValue, doubleValue) == 0;
    }
    return Double.isFinite(doubleValue) && new BigDecimal(doubleValue).compareTo(BigDecimal.valueOf(longValue)) == 0;
  }

  /// Returns `null` for NaN and infinity.
  @Nullable
  private static BigDecimal toBigDecimal(Number number) {
    if (number instanceof BigDecimal) {
      return (BigDecimal) number;
    }
    if (isFloatingPoint(number)) {
      double value = number.doubleValue();
      return Double.isFinite(value) ? BigDecimal.valueOf(value) : null;
    }
    return BigDecimal.valueOf(number.longValue());
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, isVarArg = true)
  public static Object coalesce(Object... objects) {
    for (Object o : objects) {
      if (o != null) {
        return o;
      }
    }
    return null;
  }

  @Nullable
  @ScalarFunction(names = {"case", "caseWhen"}, nullableParameters = true, isVarArg = true)
  public static Object caseWhen(Object... objs) {
    for (int i = 0; i < objs.length - 1; i += 2) {
      if (Boolean.TRUE.equals(objs[i])) {
        return objs[i + 1];
      }
    }
    // with or without else statement.
    return objs.length % 2 == 0 ? null : objs[objs.length - 1];
  }

  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Object nullIf(@Nullable Object obj1, @Nullable Object obj2) {
    if (obj1 == null) {
      return null;
    } else {
      return obj1.equals(obj2) ? null : obj1;
    }
  }
}
