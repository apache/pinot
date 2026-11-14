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
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;


/// Stateless scalar functions for the Pinot {@code VARIANT} logical type. All methods are thread-safe.
public final class VariantFunctions {
  private VariantFunctions() {
  }

  @Nullable
  @ScalarFunction
  public static byte[] variantGet(byte[] variant, String path) {
    return VariantUtils.variantGet(variant, path);
  }

  @Nullable
  @ScalarFunction
  public static Object variantGet(byte[] variant, String path, String targetType) {
    return VariantUtils.variantGet(variant, path, targetType);
  }

  @Nullable
  @ScalarFunction
  public static byte[] tryVariantGet(byte[] variant, String path) {
    return VariantUtils.tryVariantGet(variant, path);
  }

  @Nullable
  @ScalarFunction
  public static Object tryVariantGet(byte[] variant, String path, String targetType) {
    return VariantUtils.tryVariantGet(variant, path, targetType);
  }

  @Nullable
  @ScalarFunction
  public static Boolean variantExists(byte[] variant, String path) {
    return VariantUtils.variantExists(variant, path);
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isVariantNull(@Nullable byte[] variant) {
    return VariantUtils.isVariantNull(variant);
  }

  @ScalarFunction(nullableParameters = true)
  public static boolean isVariantNull(@Nullable byte[] variant, String path) {
    return VariantUtils.isVariantNull(variant, path);
  }

  @Nullable
  @ScalarFunction
  public static String variantTypeOf(byte[] variant) {
    return VariantUtils.variantTypeOf(variant);
  }

  @Nullable
  @ScalarFunction
  public static String variantTypeOf(byte[] variant, String path) {
    return VariantUtils.variantTypeOf(variant, path);
  }

  @Nullable
  @ScalarFunction
  public static String variantToJson(byte[] variant) {
    return VariantUtils.variantToJson(variant);
  }

  @Nullable
  @ScalarFunction(names = {"parseJsonToVariant", "parseJson"})
  public static byte[] parseJsonToVariant(String json) {
    return VariantUtils.parseJsonToVariant(json);
  }

  @Nullable
  @ScalarFunction(names = {"tryParseJsonToVariant", "tryParseJson"})
  public static byte[] tryParseJsonToVariant(String json) {
    return VariantUtils.tryParseJsonToVariant(json);
  }
}
