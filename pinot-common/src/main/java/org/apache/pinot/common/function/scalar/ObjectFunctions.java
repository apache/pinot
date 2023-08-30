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

import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class ObjectFunctions {
  private ObjectFunctions() {
  }

  @ScalarFunction(nullableParameters = true, names = {"isNull", "is_null"})
  public static boolean isNull(@Nullable Object obj) {
    return obj == null;
  }

  @ScalarFunction(nullableParameters = true, names = {"isNotNull", "is_not_null"})
  public static boolean isNotNull(@Nullable Object obj) {
    return !isNull(obj);
  }

  @ScalarFunction(nullableParameters = true, names = {"isDistinctFrom", "is_distinct_from"})
  public static boolean isDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    if (obj1 == null && obj2 == null) {
      return false;
    }
    if (obj1 == null || obj2 == null) {
      return true;
    }
    return !obj1.equals(obj2);
  }

  @ScalarFunction(nullableParameters = true, names = {"isNotDistinctFrom", "is_not_distinct_from"})
  public static boolean isNotDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    return !isDistinctFrom(obj1, obj2);
  }

  @Nullable
  public static Object coalesce(@Nullable Object obj) {
    return coalesceVar(obj);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2) {
    return coalesceVar(obj1, obj2);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3) {
    return coalesceVar(obj1, obj2, obj3);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3,
      @Nullable Object obj4) {
    return coalesceVar(obj1, obj2, obj3, obj4);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3,
      @Nullable Object obj4, @Nullable Object obj5) {
    return coalesceVar(obj1, obj2, obj3, obj4, obj5);
  }

  @Nullable
  private static Object coalesceVar(Object... objects) {
    for (Object o : objects) {
      if (o != null) {
        return o;
      }
    }
    return null;
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, names = {"case", "caseWhen", "case_when"})
  public static Object caseWhen(@Nullable Boolean c1, @Nullable Object o1, @Nullable Object oe) {
    return caseWhenVar(c1, o1, oe);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, names = {"case", "caseWhen", "case_when"})
  public static Object caseWhen(@Nullable Boolean c1, @Nullable Object o1, @Nullable Boolean c2, @Nullable Object o2,
      @Nullable Object oe) {
    return caseWhenVar(c1, o1, c2, o2, oe);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, names = {"case", "caseWhen", "case_when"})
  public static Object caseWhen(@Nullable Boolean c1, @Nullable Object o1, @Nullable Boolean c2, @Nullable Object o2,
      @Nullable Boolean c3, @Nullable Object o3, @Nullable Object oe) {
    return caseWhenVar(c1, o1, c2, o2, c3, o3, oe);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, names = {"case", "caseWhen", "case_when"})
  public static Object caseWhen(@Nullable Boolean c1, @Nullable Object o1, @Nullable Boolean c2, @Nullable Object o2,
      @Nullable Boolean c3, @Nullable Object o3, @Nullable Boolean c4, @Nullable Object o4, @Nullable Object oe) {
    return caseWhenVar(c1, o1, c2, o2, c3, o3, c4, o4, oe);
  }

  @Nullable
  @ScalarFunction(nullableParameters = true, names = {"case", "caseWhen", "case_when"})
  public static Object caseWhen(@Nullable Boolean c1, @Nullable Object o1, @Nullable Boolean c2, @Nullable Object o2,
      @Nullable Boolean c3, @Nullable Object o3, @Nullable Boolean c4, @Nullable Object o4, @Nullable Boolean c5,
      @Nullable Object o5, @Nullable Object oe) {
    return caseWhenVar(c1, o1, c2, o2, c3, o3, c4, o4, c5, o5, oe);
  }

  @Nullable
  private static Object caseWhenVar(Object... objs) {
    for (int i = 0; i < objs.length - 1; i += 2) {
      if (Boolean.TRUE.equals(objs[i])) {
        return objs[i + 1];
      }
    }
    // with or without else statement.
    return objs.length % 2 == 0 ? null : objs[objs.length - 1];
  }
}
