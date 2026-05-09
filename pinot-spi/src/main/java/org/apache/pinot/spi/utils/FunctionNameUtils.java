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
package org.apache.pinot.spi.utils;

import java.util.Locale;
import org.apache.commons.lang3.StringUtils;


/// Utilities for normalizing function names across Pinot.
///
/// This is the single authoritative implementation used by `FunctionRegistry`,
/// `ScalarFunctionUtils`, and other function-resolution paths throughout the codebase.
public final class FunctionNameUtils {
  private FunctionNameUtils() {
  }

  /// Returns the canonical form of a function name: underscores stripped, lower-cased.
  ///
  /// Both scalar functions and aggregate functions use this normalization so that
  /// `fnv1a_32`, `FNV1A32`, and `fnv1a32` all resolve to the same canonical name.
  public static String canonicalize(String name) {
    return StringUtils.remove(name, '_').toLowerCase(Locale.ROOT);
  }
}
