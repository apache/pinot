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

  // ==================== Tests for logicalAnd ====================

  @Test
  public void testLogicalAndTrueTrue() {
    Boolean result = LogicalFunctions.logicalAnd(true, true);
    assertTrue(result);
  }

  @Test
  public void testLogicalAndTrueFalse() {
    Boolean result = LogicalFunctions.logicalAnd(true, false);
    assertFalse(result);
  }

  @Test
  public void testLogicalAndFalseTrue() {
    Boolean result = LogicalFunctions.logicalAnd(false, true);
    assertFalse(result);
  }

  @Test
  public void testLogicalAndFalseFalse() {
    Boolean result = LogicalFunctions.logicalAnd(false, false);
    assertFalse(result);
  }

  @Test
  public void testLogicalAndTrueNull() {
    Boolean result = LogicalFunctions.logicalAnd(true, null);
    assertNull(result, "AND with TRUE and NULL should return NULL");
  }

  @Test
  public void testLogicalAndNullTrue() {
    Boolean result = LogicalFunctions.logicalAnd(null, true);
    assertNull(result, "AND with NULL and TRUE should return NULL");
  }

  @Test
  public void testLogicalAndFalseNull() {
    Boolean result = LogicalFunctions.logicalAnd(false, null);
    assertFalse(result, "AND with FALSE and NULL should return FALSE");
  }

  @Test
  public void testLogicalAndNullFalse() {
    Boolean result = LogicalFunctions.logicalAnd(null, false);
    assertFalse(result, "AND with NULL and FALSE should return FALSE");
  }

  @Test
  public void testLogicalAndNullNull() {
    Boolean result = LogicalFunctions.logicalAnd(null, null);
    assertNull(result, "AND with NULL and NULL should return NULL");
  }

  // ==================== Tests for logicalOr ====================

  @Test
  public void testLogicalOrTrueTrue() {
    Boolean result = LogicalFunctions.logicalOr(true, true);
    assertTrue(result);
  }

  @Test
  public void testLogicalOrTrueFalse() {
    Boolean result = LogicalFunctions.logicalOr(true, false);
    assertTrue(result);
  }

  @Test
  public void testLogicalOrFalseTrue() {
    Boolean result = LogicalFunctions.logicalOr(false, true);
    assertTrue(result);
  }

  @Test
  public void testLogicalOrFalseFalse() {
    Boolean result = LogicalFunctions.logicalOr(false, false);
    assertFalse(result);
  }

  @Test
  public void testLogicalOrTrueNull() {
    Boolean result = LogicalFunctions.logicalOr(true, null);
    assertTrue(result, "OR with TRUE and NULL should return TRUE");
  }

  @Test
  public void testLogicalOrNullTrue() {
    Boolean result = LogicalFunctions.logicalOr(null, true);
    assertTrue(result, "OR with NULL and TRUE should return TRUE");
  }

  @Test
  public void testLogicalOrFalseNull() {
    Boolean result = LogicalFunctions.logicalOr(false, null);
    assertNull(result, "OR with FALSE and NULL should return NULL");
  }

  @Test
  public void testLogicalOrNullFalse() {
    Boolean result = LogicalFunctions.logicalOr(null, false);
    assertNull(result, "OR with NULL and FALSE should return NULL");
  }

  @Test
  public void testLogicalOrNullNull() {
    Boolean result = LogicalFunctions.logicalOr(null, null);
    assertNull(result, "OR with NULL and NULL should return NULL");
  }

  // ==================== Tests for logicalNot ====================

  @Test
  public void testLogicalNotTrue() {
    Boolean result = LogicalFunctions.logicalNot(true);
    assertFalse(result);
  }

  @Test
  public void testLogicalNotFalse() {
    Boolean result = LogicalFunctions.logicalNot(false);
    assertTrue(result);
  }

  @Test
  public void testLogicalNotNull() {
    Boolean result = LogicalFunctions.logicalNot(null);
    assertNull(result, "NOT with NULL should return NULL");
  }

  // ==================== Complex Combination Tests ====================

  @Test
  public void testComplexAndOrCombinations() {
    // (TRUE AND FALSE) OR TRUE = FALSE OR TRUE = TRUE
    Boolean andResult = LogicalFunctions.logicalAnd(true, false);
    Boolean finalResult = LogicalFunctions.logicalOr(andResult, true);
    assertTrue(finalResult);

    // (TRUE OR FALSE) AND FALSE = TRUE AND FALSE = FALSE
    Boolean orResult = LogicalFunctions.logicalOr(true, false);
    finalResult = LogicalFunctions.logicalAnd(orResult, false);
    assertFalse(finalResult);

    // (NULL AND TRUE) OR FALSE = NULL OR FALSE = NULL
    andResult = LogicalFunctions.logicalAnd(null, true);
    finalResult = LogicalFunctions.logicalOr(andResult, false);
    assertNull(finalResult);

    // (NULL OR TRUE) AND TRUE = TRUE AND TRUE = TRUE
    orResult = LogicalFunctions.logicalOr(null, true);
    finalResult = LogicalFunctions.logicalAnd(orResult, true);
    assertTrue(finalResult);
  }

  @Test
  public void testComplexNotCombinations() {
    // NOT(TRUE AND FALSE) = NOT(FALSE) = TRUE
    Boolean andResult = LogicalFunctions.logicalAnd(true, false);
    Boolean notResult = LogicalFunctions.logicalNot(andResult);
    assertTrue(notResult);

    // NOT(TRUE OR FALSE) = NOT(TRUE) = FALSE
    Boolean orResult = LogicalFunctions.logicalOr(true, false);
    notResult = LogicalFunctions.logicalNot(orResult);
    assertFalse(notResult);

    // NOT(NULL AND TRUE) = NOT(NULL) = NULL
    andResult = LogicalFunctions.logicalAnd(null, true);
    notResult = LogicalFunctions.logicalNot(andResult);
    assertNull(notResult);

    // NOT(NULL OR FALSE) = NOT(NULL) = NULL
    orResult = LogicalFunctions.logicalOr(null, false);
    notResult = LogicalFunctions.logicalNot(orResult);
    assertNull(notResult);
  }

  @Test
  public void testDeMorgansLaws() {
    // De Morgan's Law: NOT(A AND B) = NOT(A) OR NOT(B)
    // Test with TRUE and FALSE
    Boolean notAndResult = LogicalFunctions.logicalNot(LogicalFunctions.logicalAnd(true, false));
    Boolean notAOrNotB = LogicalFunctions.logicalOr(
        LogicalFunctions.logicalNot(true),
        LogicalFunctions.logicalNot(false)
    );
    assertEquals(notAndResult, notAOrNotB);

    // De Morgan's Law: NOT(A OR B) = NOT(A) AND NOT(B)
    // Test with TRUE and FALSE
    Boolean notOrResult = LogicalFunctions.logicalNot(LogicalFunctions.logicalOr(true, false));
    Boolean notAAndNotB = LogicalFunctions.logicalAnd(
        LogicalFunctions.logicalNot(true),
        LogicalFunctions.logicalNot(false)
    );
    assertEquals(notOrResult, notAAndNotB);
  }

  // ==================== Edge Cases and Truth Table Verification ====================

  @Test
  public void testAndTruthTableCompleteness() {
    // Verify all 9 combinations in the AND truth table
    assertTrue(LogicalFunctions.logicalAnd(true, true));
    assertFalse(LogicalFunctions.logicalAnd(true, false));
    assertNull(LogicalFunctions.logicalAnd(true, null));

    assertFalse(LogicalFunctions.logicalAnd(false, true));
    assertFalse(LogicalFunctions.logicalAnd(false, false));
    assertFalse(LogicalFunctions.logicalAnd(false, null));

    assertNull(LogicalFunctions.logicalAnd(null, true));
    assertFalse(LogicalFunctions.logicalAnd(null, false));
    assertNull(LogicalFunctions.logicalAnd(null, null));
  }

  @Test
  public void testOrTruthTableCompleteness() {
    // Verify all 9 combinations in the OR truth table
    assertTrue(LogicalFunctions.logicalOr(true, true));
    assertTrue(LogicalFunctions.logicalOr(true, false));
    assertTrue(LogicalFunctions.logicalOr(true, null));

    assertTrue(LogicalFunctions.logicalOr(false, true));
    assertFalse(LogicalFunctions.logicalOr(false, false));
    assertNull(LogicalFunctions.logicalOr(false, null));

    assertTrue(LogicalFunctions.logicalOr(null, true));
    assertNull(LogicalFunctions.logicalOr(null, false));
    assertNull(LogicalFunctions.logicalOr(null, null));
  }

  @Test
  public void testNotTruthTableCompleteness() {
    // Verify all 3 combinations in the NOT truth table
    assertFalse(LogicalFunctions.logicalNot(true));
    assertTrue(LogicalFunctions.logicalNot(false));
    assertNull(LogicalFunctions.logicalNot(null));
  }

  // ==================== Commutative Property Tests ====================

  @Test
  public void testAndCommutativeProperty() {
    // AND should be commutative: A AND B = B AND A
    assertEquals(
        LogicalFunctions.logicalAnd(true, false),
        LogicalFunctions.logicalAnd(false, true)
    );

    assertEquals(
        LogicalFunctions.logicalAnd(true, null),
        LogicalFunctions.logicalAnd(null, true)
    );

    assertEquals(
        LogicalFunctions.logicalAnd(false, null),
        LogicalFunctions.logicalAnd(null, false)
    );
  }

  @Test
  public void testOrCommutativeProperty() {
    // OR should be commutative: A OR B = B OR A
    assertEquals(
        LogicalFunctions.logicalOr(true, false),
        LogicalFunctions.logicalOr(false, true)
    );

    assertEquals(
        LogicalFunctions.logicalOr(true, null),
        LogicalFunctions.logicalOr(null, true)
    );

    assertEquals(
        LogicalFunctions.logicalOr(false, null),
        LogicalFunctions.logicalOr(null, false)
    );
  }

  // ==================== Associative Property Tests ====================

  @Test
  public void testAndAssociativeProperty() {
    // AND should be associative: (A AND B) AND C = A AND (B AND C)
    Boolean left = LogicalFunctions.logicalAnd(
        LogicalFunctions.logicalAnd(true, false),
        true
    );
    Boolean right = LogicalFunctions.logicalAnd(
        true,
        LogicalFunctions.logicalAnd(false, true)
    );
    assertEquals(left, right);

    // Test with NULL
    left = LogicalFunctions.logicalAnd(
        LogicalFunctions.logicalAnd(true, null),
        false
    );
    right = LogicalFunctions.logicalAnd(
        true,
        LogicalFunctions.logicalAnd(null, false)
    );
    assertEquals(left, right);
  }

  @Test
  public void testOrAssociativeProperty() {
    // OR should be associative: (A OR B) OR C = A OR (B OR C)
    Boolean left = LogicalFunctions.logicalOr(
        LogicalFunctions.logicalOr(false, false),
        true
    );
    Boolean right = LogicalFunctions.logicalOr(
        false,
        LogicalFunctions.logicalOr(false, true)
    );
    assertEquals(left, right);

    // Test with NULL
    left = LogicalFunctions.logicalOr(
        LogicalFunctions.logicalOr(false, null),
        true
    );
    right = LogicalFunctions.logicalOr(
        false,
        LogicalFunctions.logicalOr(null, true)
    );
    assertEquals(left, right);
  }

  // ==================== Identity and Absorption Tests ====================

  @Test
  public void testAndIdentityElement() {
    // TRUE is the identity element for AND: A AND TRUE = A
    assertEquals(LogicalFunctions.logicalAnd(true, true), true);
    assertEquals(LogicalFunctions.logicalAnd(false, true), false);
    assertEquals(LogicalFunctions.logicalAnd(null, true), null);
  }

  @Test
  public void testOrIdentityElement() {
    // FALSE is the identity element for OR: A OR FALSE = A
    assertEquals(LogicalFunctions.logicalOr(true, false), true);
    assertEquals(LogicalFunctions.logicalOr(false, false), false);
    assertEquals(LogicalFunctions.logicalOr(null, false), null);
  }

  @Test
  public void testAndAbsorptionElement() {
    // FALSE is the absorption element for AND: A AND FALSE = FALSE
    assertFalse(LogicalFunctions.logicalAnd(true, false));
    assertFalse(LogicalFunctions.logicalAnd(false, false));
    assertFalse(LogicalFunctions.logicalAnd(null, false));
  }

  @Test
  public void testOrAbsorptionElement() {
    // TRUE is the absorption element for OR: A OR TRUE = TRUE
    assertTrue(LogicalFunctions.logicalOr(true, true));
    assertTrue(LogicalFunctions.logicalOr(false, true));
    assertTrue(LogicalFunctions.logicalOr(null, true));
  }

  // ==================== Double Negation Tests ====================

  @Test
  public void testDoubleNegation() {
    // NOT(NOT(A)) = A
    assertEquals(
        LogicalFunctions.logicalNot(LogicalFunctions.logicalNot(true)),
        true
    );
    assertEquals(
        LogicalFunctions.logicalNot(LogicalFunctions.logicalNot(false)),
        false
    );
    assertNull(LogicalFunctions.logicalNot(LogicalFunctions.logicalNot(null)));
  }

  // ==================== Trino Compatibility Examples ====================

  @Test
  public void testTrinoExampleFromDocumentation() {
    // Examples from Trino documentation:
    // CAST(null AS boolean) AND true → null
    assertNull(LogicalFunctions.logicalAnd(null, true));

    // CAST(null AS boolean) AND false → false
    assertFalse(LogicalFunctions.logicalAnd(null, false));

    // CAST(null AS boolean) AND CAST(null AS boolean) → null
    assertNull(LogicalFunctions.logicalAnd(null, null));

    // CAST(null AS boolean) OR CAST(null AS boolean) → null
    assertNull(LogicalFunctions.logicalOr(null, null));

    // CAST(null AS boolean) OR false → null
    assertNull(LogicalFunctions.logicalOr(null, false));

    // CAST(null AS boolean) OR true → true
    assertTrue(LogicalFunctions.logicalOr(null, true));

    // NOT CAST(null AS boolean) → null
    assertNull(LogicalFunctions.logicalNot(null));
  }
}
