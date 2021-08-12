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
package org.apache.pinot.util;

import org.apache.pinot.common.utils.LikeToRegexpLikePatternConverterUtils;
import org.testng.annotations.Test;


/**
 * Tests for {@LikeToRegexFormatConverterUtil}
 */
public class TestLikeSyntaxConverter {

  private static final String TRAILING_WILDCARD = "C+%";
  private static final String LEADING_WILDCARD = "%++";
  private static final String BOTH_SIDES_WILDCARD = "%+%";
  private static final String WILD_CARD_IN_MIDDLE = "C%+";
  private static final String TRAILING_SINGLE_CHARACTER = "C+_";
  private static final String LEADING_SINGLE_CHARACTER = "_++";
  private static final String SINGLE_CHARACTER_IN_MIDDLE = "C_+";
  private static final String COMBINATION_PATTERN = "C_%";

  @Test
  public void testLeadingWildcard() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(LEADING_WILDCARD);

    assert result.equals(".*\\+\\+");
  }

  @Test
  public void testTrailingWildcard() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(TRAILING_WILDCARD);

    assert result.equals("C\\+.*");
  }

  @Test
  public void testBothSidesWildcard() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(BOTH_SIDES_WILDCARD);

    assert result.equals(".*\\+.*");
  }

  @Test
  public void testWildCardInMiddle() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(WILD_CARD_IN_MIDDLE);

    assert result.equals("C.*\\+");
  }

  @Test
  public void testTrailingSingleCharacter() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(TRAILING_SINGLE_CHARACTER);

    assert result.equals("C\\+.");
  }

  @Test
  public void testLeadingSingleCharacter() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(LEADING_SINGLE_CHARACTER);

    assert result.equals(".\\+\\+");
  }

  @Test
  public void testSingleCharacterInMiddle() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(SINGLE_CHARACTER_IN_MIDDLE);

    assert result.equals("C.\\+");
  }

  @Test
  public void testCombinationPattern() {
    String result = LikeToRegexpLikePatternConverterUtils.processValue(COMBINATION_PATTERN);

    assert result.equals("C..*");
  }
}
