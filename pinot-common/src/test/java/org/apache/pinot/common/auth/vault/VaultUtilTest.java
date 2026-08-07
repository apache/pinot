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
package org.apache.pinot.common.auth.vault;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for VaultUtil.
 * Tests utility methods for vault configuration validation.
 */
public class VaultUtilTest {

  @Test
  public void testIsNumber() {
    // Valid numbers
    assertTrue(VaultUtil.isNumber("0"));
    assertTrue(VaultUtil.isNumber("1"));
    assertTrue(VaultUtil.isNumber("123"));
    assertTrue(VaultUtil.isNumber("999999"));

    // Invalid numbers
    assertFalse(VaultUtil.isNumber(null));
    assertFalse(VaultUtil.isNumber(""));
    assertFalse(VaultUtil.isNumber("abc"));
    assertFalse(VaultUtil.isNumber("12.34"));
    assertFalse(VaultUtil.isNumber("-5"));
    assertFalse(VaultUtil.isNumber("1a2"));
  }

  @Test
  public void testIsPositive() {
    // Valid positive numbers
    assertTrue(VaultUtil.isPositive("1"));
    assertTrue(VaultUtil.isPositive("10"));
    assertTrue(VaultUtil.isPositive("100"));
    assertTrue(VaultUtil.isPositive("999999"));

    // Invalid - zero and negative
    assertFalse(VaultUtil.isPositive("0"));
    assertFalse(VaultUtil.isPositive("-1"));
    assertFalse(VaultUtil.isPositive("-10"));

    // Invalid - not numbers
    assertFalse(VaultUtil.isPositive(null));
    assertFalse(VaultUtil.isPositive(""));
    assertFalse(VaultUtil.isPositive("abc"));
    assertFalse(VaultUtil.isPositive("12.34"));
  }
}
