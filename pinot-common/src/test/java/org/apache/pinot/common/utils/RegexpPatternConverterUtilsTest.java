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
    assertEquals(regexpLikePattern, "\\+\\+$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, ".*\\+\\+");
  }

  @Test
  public void testTrailingWildcard() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(TRAILING_WILDCARD);
    assertEquals(regexpLikePattern, "^C\\+");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C\\+.*");
  }

  @Test
  public void testBothSidesWildcard() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike(BOTH_SIDES_WILDCARD);
    assertEquals(regexpLikePattern, "\\+");
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
    assertEquals(regexpLikePattern, "^C.");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "C..*");
  }

  @Test
  public void testLeadingRepeatedWildcards() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("%%%%%%%%%%%%%zz");
    assertEquals(regexpLikePattern, "zz$");
  }

  @Test
  public void testTrailingRepeatedWildcards() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("zz%%%%%%%%%%%%%");
    assertEquals(regexpLikePattern, "^zz");
  }

  @Test
  public void testLeadingSize2() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("%z");
    assertEquals(regexpLikePattern, "z$");
  }

  @Test
  public void testTrailingSize2() {
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("z%");
    assertEquals(regexpLikePattern, "^z");
  }

  @Test
  public void testEscapedWildcard1() {
    // the first underscore (_ in _b) is escaped, so it is meant to match an actual "_b" string in the provided
    // string
    // the second underscore (_ in b_) is not escaped, so it is a SQL wildcard that is used to match a single
    // character, which in the regex space is "."
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("a\\_b_\\");
    assertEquals(regexpLikePattern, "^a\\_b.\\\\$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "a\\_b.\\\\");
  }

  @Test
  public void testEscapedWildcard2() {
    // the % (% in %b) is escaped, so it is meant to match an actual "%b" string in the provided
    // string
    // the "\" before c is a normal "\", so it is meant to match an actual "\" string in the provided
    // string, this is done because "c" is not a SQL wildcard - hence the "\" before that is used as-is
    // and is not used for escaping "c"
    // so, this "\" is escaped in the output as it is a regex metacharacter and the converted regex
    // will match "a%b\cde" in the provided string
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("a\\%b\\cde");
    assertEquals(regexpLikePattern, "^a\\%b\\\\cde$");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, "a\\%b\\\\cde");
  }
  @Test
  public void testEscapedWildcard3() {
    // here the "\" character is used to escape _, so _ here is not treated as a SQL wildcard
    // but it is meant to actually match "_" in the provided string
    // so the corresponding regex doesn't convert the "_" to "."
    String regexpLikePattern = RegexpPatternConverterUtils.likeToRegexpLike("%2\\_2%");
    assertEquals(regexpLikePattern, "2\\_2");
    String luceneRegExpPattern = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePattern);
    assertEquals(luceneRegExpPattern, ".*2\\_2.*");
  }
}
