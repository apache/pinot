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
package org.apache.pinot.segment.local.utils;

import org.apache.lucene.util.automaton.Operations;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests the construction limit with small automata.
public class LuceneRegexpAutomatonTest {
  @Test
  public void testStarConstructionBudget() {
    String operand = "(ab?|cd?|ef?)";
    // The operand's initial transitions are copied to each nonterminal accepting state.
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(operand + "*", 12));
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(operand + "+", 20));
    LuceneRegexpAutomaton.validateConstructionBudget(operand + "*", 20);
    assertTrue(Operations.run(LuceneRegexpAutomaton.compile("prefix.*suffix"), "prefix-suffix"));
  }

  @Test
  public void testTransitionLinkingBudget() {
    String operand = "(ab?|cd?|ef?)";
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(operand + "{2}", 20));
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(operand + "{0,2}", 20));
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(operand + operand, 20));

    String optional = operand + "?";
    assertThrows(IllegalArgumentException.class,
        () -> LuceneRegexpAutomaton.validateConstructionBudget(optional.repeat(3), 50));
    assertTrue(Operations.run(LuceneRegexpAutomaton.compile("prefix(ab){2}.*suffix"), "prefixabab-suffix"));
  }
}
