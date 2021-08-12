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
package org.apache.pinot.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link LikeToRegexpLikePatternConverterUtils}
 */
public class LikeToRegexpLikePatternConverterUtilsTest {
  private static final String LEADING_WILDCARD = "%++";
  private static final String TRAILING_WILDCARD = "C+%";
  private static final String BOTH_SIDES_WILDCARD = "%+%";
  private static final String WILD_CARD_IN_MIDDLE = "C%+";
  private static final String LEADING_SINGLE_CHARACTER = "_++";
  private static final String TRAILING_SINGLE_CHARACTER = "C+_";
  private static final String SINGLE_CHARACTER_IN_MIDDLE = "C_+";
  private static final String COMBINATION_PATTERN = "C_%";

  @Test
  public void testLeadingWildcard() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(LEADING_WILDCARD), ".*\\+\\+");
  }

  @Test
  public void testTrailingWildcard() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(TRAILING_WILDCARD), "C\\+.*");
  }

  @Test
  public void testBothSidesWildcard() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(BOTH_SIDES_WILDCARD), ".*\\+.*");
  }

  @Test
  public void testWildCardInMiddle() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(WILD_CARD_IN_MIDDLE), "C.*\\+");
  }

  @Test
  public void testTrailingSingleCharacter() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(TRAILING_SINGLE_CHARACTER), "C\\+.");
  }

  @Test
  public void testLeadingSingleCharacter() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(LEADING_SINGLE_CHARACTER), ".\\+\\+");
  }

  @Test
  public void testSingleCharacterInMiddle() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(SINGLE_CHARACTER_IN_MIDDLE), "C.\\+");
  }

  @Test
  public void testCombinationPattern() {
    Assert.assertEquals(LikeToRegexpLikePatternConverterUtils.processValue(COMBINATION_PATTERN), "C..*");
  }
}
