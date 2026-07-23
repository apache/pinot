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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * JUnit 5 (Jupiter) test, written to run on the JUnit Platform alongside the module's
 * existing TestNG tests (which run via the testng-engine). It also serves as the reference
 * example for new Jupiter tests: {@link ParameterizedTest} replaces TestNG's {@code @DataProvider},
 * and {@link org.junit.jupiter.api.Assertions#assertThrows} replaces {@code expectedExceptions}.
 */
public class BooleanUtilsTest {

  @ParameterizedTest
  @ValueSource(strings = {"true", "TRUE", "True", "1"})
  void truthyStringsConvertToTrue(String value) {
    assertTrue(BooleanUtils.toBoolean(value));
    assertEquals(BooleanUtils.INTERNAL_TRUE, BooleanUtils.toInt(value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "FALSE", "0", "yes", "", "2"})
  void nonTruthyStringsConvertToFalse(String value) {
    assertFalse(BooleanUtils.toBoolean(value));
    assertEquals(BooleanUtils.INTERNAL_FALSE, BooleanUtils.toInt(value));
  }

  @Test
  void objectConversionCoversSupportedTypes() {
    assertFalse(BooleanUtils.toBoolean((Object) null));
    assertTrue(BooleanUtils.toBoolean((Object) "1"));
    assertTrue(BooleanUtils.toBoolean((Object) Integer.valueOf(BooleanUtils.INTERNAL_TRUE)));
    assertFalse(BooleanUtils.toBoolean((Object) Integer.valueOf(BooleanUtils.INTERNAL_FALSE)));
    assertTrue(BooleanUtils.toBoolean((Object) Boolean.TRUE));
  }

  @Test
  void unsupportedObjectTypeThrows() {
    // Not a String/Number/Boolean -> IllegalArgumentException (a Double would be treated as a Number).
    assertThrows(IllegalArgumentException.class, () -> BooleanUtils.toBoolean(new Object()));
  }

  @Test
  void internalValuePredicatesMatchConstants() {
    assertTrue(BooleanUtils.isTrueInternalValue(BooleanUtils.INTERNAL_TRUE));
    assertFalse(BooleanUtils.isTrueInternalValue(BooleanUtils.INTERNAL_FALSE));
    assertFalse(BooleanUtils.isTrueInternalValue(null));
    assertTrue(BooleanUtils.isFalseInternalValue(BooleanUtils.INTERNAL_FALSE));
    assertFalse(BooleanUtils.isFalseInternalValue(null));
    assertTrue(BooleanUtils.fromNonNullInternalValue(BooleanUtils.INTERNAL_TRUE));
  }
}
