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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for {@link RegexpPatternConverterUtils}
 */
public class RegexpPatternConverterUtilsTest {
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
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(LEADING_WILDCARD);
    assertEquals(regexpLikePattern, "^.*\\+\\+$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, ".*\\+\\+");
  }

  @Test
  public void testTrailingWildcard() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(TRAILING_WILDCARD);
    assertEquals(regexpLikePattern, "^C\\+.*$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C\\+.*");
  }

  @Test
  public void testBothSidesWildcard() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(BOTH_SIDES_WILDCARD);
    assertEquals(regexpLikePattern, "^.*\\+.*$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, ".*\\+.*");
  }

  @Test
  public void testWildCardInMiddle() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(WILD_CARD_IN_MIDDLE);
    assertEquals(regexpLikePattern, "^C.*\\+$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C.*\\+");
  }

  @Test
  public void testTrailingSingleCharacter() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(TRAILING_SINGLE_CHARACTER);
    assertEquals(regexpLikePattern, "^C\\+.$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C\\+.");
  }

  @Test
  public void testLeadingSingleCharacter() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(LEADING_SINGLE_CHARACTER);
    assertEquals(regexpLikePattern, "^.\\+\\+$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, ".\\+\\+");
    assertEquals(RegexpPatternConverterUtils.likeToRegexpLike(LEADING_SINGLE_CHARACTER), "^.\\+\\+$");
  }

  @Test
  public void testSingleCharacterInMiddle() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(SINGLE_CHARACTER_IN_MIDDLE);
    assertEquals(regexpLikePattern, "^C.\\+$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C.\\+");
    assertEquals(RegexpPatternConverterUtils.likeToRegexpLike(SINGLE_CHARACTER_IN_MIDDLE), "^C.\\+$");
  }

  @Test
  public void testCombinationPattern() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(COMBINATION_PATTERN);
    assertEquals(regexpLikePattern, "^C..*$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C..*");
  }
}
