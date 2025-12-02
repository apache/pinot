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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for Logical functions with Trino-compatible NULL handling
 */
public class LogicalFunctionsTest {

  // ==================== Tests for and ====================

  @Test
  public void testAndTrueTrue() {
    Boolean result = LogicalFunctions.and(true, true);
    assertTrue(result);
  }

  @Test
  public void testAndTrueFalse() {
    Boolean result = LogicalFunctions.and(true, false);
    assertFalse(result);
  }

  @Test
  public void testAndFalseTrue() {
    Boolean result = LogicalFunctions.and(false, true);
    assertFalse(result);
  }

  @Test
  public void testAndFalseFalse() {
    Boolean result = LogicalFunctions.and(false, false);
    assertFalse(result);
  }

  @Test
  public void testAndTrueNull() {
    Boolean result = LogicalFunctions.and(true, null);
    assertNull(result, "AND with TRUE and NULL should return NULL");
  }

  @Test
  public void testAndNullTrue() {
    Boolean result = LogicalFunctions.and(null, true);
    assertNull(result, "AND with NULL and TRUE should return NULL");
  }

  @Test
  public void testAndFalseNull() {
    Boolean result = LogicalFunctions.and(false, null);
    assertFalse(result, "AND with FALSE and NULL should return FALSE");
  }

  @Test
  public void testAndNullFalse() {
    Boolean result = LogicalFunctions.and(null, false);
    assertFalse(result, "AND with NULL and FALSE should return FALSE");
  }

  @Test
  public void testAndNullNull() {
    Boolean result = LogicalFunctions.and(null, null);
    assertNull(result, "AND with NULL and NULL should return NULL");
  }

  // ==================== Tests for or ====================

  @Test
  public void testOrTrueTrue() {
    Boolean result = LogicalFunctions.or(true, true);
    assertTrue(result);
  }

  @Test
  public void testOrTrueFalse() {
    Boolean result = LogicalFunctions.or(true, false);
    assertTrue(result);
  }

  @Test
  public void testOrFalseTrue() {
    Boolean result = LogicalFunctions.or(false, true);
    assertTrue(result);
  }

  @Test
  public void testOrFalseFalse() {
    Boolean result = LogicalFunctions.or(false, false);
    assertFalse(result);
  }

  @Test
  public void testOrTrueNull() {
    Boolean result = LogicalFunctions.or(true, null);
    assertTrue(result, "OR with TRUE and NULL should return TRUE");
  }

  @Test
  public void testOrNullTrue() {
    Boolean result = LogicalFunctions.or(null, true);
    assertTrue(result, "OR with NULL and TRUE should return TRUE");
  }

  @Test
  public void testOrFalseNull() {
    Boolean result = LogicalFunctions.or(false, null);
    assertNull(result, "OR with FALSE and NULL should return NULL");
  }

  @Test
  public void testOrNullFalse() {
    Boolean result = LogicalFunctions.or(null, false);
    assertNull(result, "OR with NULL and FALSE should return NULL");
  }

  @Test
  public void testOrNullNull() {
    Boolean result = LogicalFunctions.or(null, null);
    assertNull(result, "OR with NULL and NULL should return NULL");
  }

  // ==================== Tests for not ====================

  @Test
  public void testNotTrue() {
    Boolean result = LogicalFunctions.not(true);
    assertFalse(result);
  }

  @Test
  public void testNotFalse() {
    Boolean result = LogicalFunctions.not(false);
    assertTrue(result);
  }

  @Test
  public void testNotNull() {
    Boolean result = LogicalFunctions.not(null);
    assertNull(result, "NOT with NULL should return NULL");
  }

  // ==================== Complex Combination Tests ====================

  @Test
  public void testComplexAndOrCombinations() {
    // (TRUE AND FALSE) OR TRUE = FALSE OR TRUE = TRUE
    Boolean andResult = LogicalFunctions.and(true, false);
    Boolean finalResult = LogicalFunctions.or(andResult, true);
    assertTrue(finalResult);

    // (TRUE OR FALSE) AND FALSE = TRUE AND FALSE = FALSE
    Boolean orResult = LogicalFunctions.or(true, false);
    finalResult = LogicalFunctions.and(orResult, false);
    assertFalse(finalResult);

    // (NULL AND TRUE) OR FALSE = NULL OR FALSE = NULL
    andResult = LogicalFunctions.and(null, true);
    finalResult = LogicalFunctions.or(andResult, false);
    assertNull(finalResult);

    // (NULL OR TRUE) AND TRUE = TRUE AND TRUE = TRUE
    orResult = LogicalFunctions.or(null, true);
    finalResult = LogicalFunctions.and(orResult, true);
    assertTrue(finalResult);
  }

  @Test
  public void testComplexNotCombinations() {
    // NOT(TRUE AND FALSE) = NOT(FALSE) = TRUE
    Boolean andResult = LogicalFunctions.and(true, false);
    Boolean notResult = LogicalFunctions.not(andResult);
    assertTrue(notResult);

    // NOT(TRUE OR FALSE) = NOT(TRUE) = FALSE
    Boolean orResult = LogicalFunctions.or(true, false);
    notResult = LogicalFunctions.not(orResult);
    assertFalse(notResult);

    // NOT(NULL AND TRUE) = NOT(NULL) = NULL
    andResult = LogicalFunctions.and(null, true);
    notResult = LogicalFunctions.not(andResult);
    assertNull(notResult);

    // NOT(NULL OR FALSE) = NOT(NULL) = NULL
    orResult = LogicalFunctions.or(null, false);
    notResult = LogicalFunctions.not(orResult);
    assertNull(notResult);
  }

  @Test
  public void testDeMorgansLaws() {
    // De Morgan's Law: NOT(A AND B) = NOT(A) OR NOT(B)
    // Test with TRUE and FALSE
    Boolean notAndResult = LogicalFunctions.not(LogicalFunctions.and(true, false));
    Boolean notAOrNotB = LogicalFunctions.or(
        LogicalFunctions.not(true),
        LogicalFunctions.not(false)
    );
    assertEquals(notAndResult, notAOrNotB);

    // De Morgan's Law: NOT(A OR B) = NOT(A) AND NOT(B)
    // Test with TRUE and FALSE
    Boolean notOrResult = LogicalFunctions.not(LogicalFunctions.or(true, false));
    Boolean notAAndNotB = LogicalFunctions.and(
        LogicalFunctions.not(true),
        LogicalFunctions.not(false)
    );
    assertEquals(notOrResult, notAAndNotB);
  }

  // ==================== Edge Cases and Truth Table Verification ====================

  @Test
  public void testAndTruthTableCompleteness() {
    // Verify all 9 combinations in the AND truth table
    assertTrue(LogicalFunctions.and(true, true));
    assertFalse(LogicalFunctions.and(true, false));
    assertNull(LogicalFunctions.and(true, null));

    assertFalse(LogicalFunctions.and(false, true));
    assertFalse(LogicalFunctions.and(false, false));
    assertFalse(LogicalFunctions.and(false, null));

    assertNull(LogicalFunctions.and(null, true));
    assertFalse(LogicalFunctions.and(null, false));
    assertNull(LogicalFunctions.and(null, null));
  }

  @Test
  public void testOrTruthTableCompleteness() {
    // Verify all 9 combinations in the OR truth table
    assertTrue(LogicalFunctions.or(true, true));
    assertTrue(LogicalFunctions.or(true, false));
    assertTrue(LogicalFunctions.or(true, null));

    assertTrue(LogicalFunctions.or(false, true));
    assertFalse(LogicalFunctions.or(false, false));
    assertNull(LogicalFunctions.or(false, null));

    assertTrue(LogicalFunctions.or(null, true));
    assertNull(LogicalFunctions.or(null, false));
    assertNull(LogicalFunctions.or(null, null));
  }

  @Test
  public void testNotTruthTableCompleteness() {
    // Verify all 3 combinations in the NOT truth table
    assertFalse(LogicalFunctions.not(true));
    assertTrue(LogicalFunctions.not(false));
    assertNull(LogicalFunctions.not(null));
  }

  // ==================== Commutative Property Tests ====================

  @Test
  public void testAndCommutativeProperty() {
    // AND should be commutative: A AND B = B AND A
    assertEquals(
        LogicalFunctions.and(true, false),
        LogicalFunctions.and(false, true)
    );

    assertEquals(
        LogicalFunctions.and(true, null),
        LogicalFunctions.and(null, true)
    );

    assertEquals(
        LogicalFunctions.and(false, null),
        LogicalFunctions.and(null, false)
    );
  }

  @Test
  public void testOrCommutativeProperty() {
    // OR should be commutative: A OR B = B OR A
    assertEquals(
        LogicalFunctions.or(true, false),
        LogicalFunctions.or(false, true)
    );

    assertEquals(
        LogicalFunctions.or(true, null),
        LogicalFunctions.or(null, true)
    );

    assertEquals(
        LogicalFunctions.or(false, null),
        LogicalFunctions.or(null, false)
    );
  }

  // ==================== Associative Property Tests ====================

  @Test
  public void testAndAssociativeProperty() {
    // AND should be associative: (A AND B) AND C = A AND (B AND C)
    Boolean left = LogicalFunctions.and(
        LogicalFunctions.and(true, false),
        true
    );
    Boolean right = LogicalFunctions.and(
        true,
        LogicalFunctions.and(false, true)
    );
    assertEquals(left, right);

    // Test with NULL
    left = LogicalFunctions.and(
        LogicalFunctions.and(true, null),
        false
    );
    right = LogicalFunctions.and(
        true,
        LogicalFunctions.and(null, false)
    );
    assertEquals(left, right);
  }

  @Test
  public void testOrAssociativeProperty() {
    // OR should be associative: (A OR B) OR C = A OR (B OR C)
    Boolean left = LogicalFunctions.or(
        LogicalFunctions.or(false, false),
        true
    );
    Boolean right = LogicalFunctions.or(
        false,
        LogicalFunctions.or(false, true)
    );
    assertEquals(left, right);

    // Test with NULL
    left = LogicalFunctions.or(
        LogicalFunctions.or(false, null),
        true
    );
    right = LogicalFunctions.or(
        false,
        LogicalFunctions.or(null, true)
    );
    assertEquals(left, right);
  }

  // ==================== Identity and Absorption Tests ====================

  @Test
  public void testAndIdentityElement() {
    // TRUE is the identity element for AND: A AND TRUE = A
    assertEquals(LogicalFunctions.and(true, true), true);
    assertEquals(LogicalFunctions.and(false, true), false);
    assertEquals(LogicalFunctions.and(null, true), null);
  }

  @Test
  public void testOrIdentityElement() {
    // FALSE is the identity element for OR: A OR FALSE = A
    assertEquals(LogicalFunctions.or(true, false), true);
    assertEquals(LogicalFunctions.or(false, false), false);
    assertEquals(LogicalFunctions.or(null, false), null);
  }

  @Test
  public void testAndAbsorptionElement() {
    // FALSE is the absorption element for AND: A AND FALSE = FALSE
    assertFalse(LogicalFunctions.and(true, false));
    assertFalse(LogicalFunctions.and(false, false));
    assertFalse(LogicalFunctions.and(null, false));
  }

  @Test
  public void testOrAbsorptionElement() {
    // TRUE is the absorption element for OR: A OR TRUE = TRUE
    assertTrue(LogicalFunctions.or(true, true));
    assertTrue(LogicalFunctions.or(false, true));
    assertTrue(LogicalFunctions.or(null, true));
  }

  // ==================== Double Negation Tests ====================

  @Test
  public void testDoubleNegation() {
    // NOT(NOT(A)) = A
    assertEquals(
        LogicalFunctions.not(LogicalFunctions.not(true)),
        true
    );
    assertEquals(
        LogicalFunctions.not(LogicalFunctions.not(false)),
        false
    );
    assertNull(LogicalFunctions.not(LogicalFunctions.not(null)));
  }

  // ==================== Trino Compatibility Examples ====================

  @Test
  public void testTrinoExampleFromDocumentation() {
    // Examples from Trino documentation:
    // CAST(null AS boolean) AND true → null
    assertNull(LogicalFunctions.and(null, true));

    // CAST(null AS boolean) AND false → false
    assertFalse(LogicalFunctions.and(null, false));

    // CAST(null AS boolean) AND CAST(null AS boolean) → null
    assertNull(LogicalFunctions.and(null, null));

    // CAST(null AS boolean) OR CAST(null AS boolean) → null
    assertNull(LogicalFunctions.or(null, null));

    // CAST(null AS boolean) OR false → null
    assertNull(LogicalFunctions.or(null, false));

    // CAST(null AS boolean) OR true → true
    assertTrue(LogicalFunctions.or(null, true));

    // NOT CAST(null AS boolean) → null
    assertNull(LogicalFunctions.not(null));
  }
}
