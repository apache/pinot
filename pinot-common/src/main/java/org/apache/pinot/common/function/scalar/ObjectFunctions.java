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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;

public class ObjectFunctions {
  private ObjectFunctions() {
  }

  private static Object coalesce(List<Object> objects) {
    for (Object o : objects) {
      if (o != null) {
        return o;
      }
    }
    return null;
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNull(@Nullable Object obj) {
    return obj == null;
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNotNull(@Nullable Object obj) {
    return !isNull(obj);
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    if (obj1 == null && obj2 == null) {
      return false;
    }
    if (obj1 == null || obj2 == null) {
      return true;
    }
    return !obj1.equals(obj2);
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isNotDistinctFrom(@Nullable Object obj1, @Nullable Object obj2) {
    return !isDistinctFrom(obj1, obj2);
  }

  public static Object coalesce(@Nullable Object obj) {
    return coalesce(Arrays.asList(obj));
  }

  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2) {
    return coalesce(Arrays.asList(obj1, obj2));
  }

  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3) {
    return coalesce(Arrays.asList(obj1, obj2, obj3));
  }

  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3,
      @Nullable Object obj4) {
    return coalesce(Arrays.asList(obj1, obj2, obj3, obj4));
  }

  @ScalarFunction(nullableParameters = true)
  public static Object coalesce(@Nullable Object obj1, @Nullable Object obj2, @Nullable Object obj3,
      @Nullable Object obj4, @Nullable Object obj5) {
    return coalesce(Arrays.asList(obj1, obj2, obj3, obj4, obj5));
  }
}
