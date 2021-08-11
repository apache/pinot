package org.apache.pinot.util;

import org.apache.pinot.common.utils.LikeToRegexFormatConverterUtil;
import org.testng.annotations.Test;


/**
 * Tests for {@LikeToRegexFormatConverterUtil}
 */
public class TestLikeSyntaxConverter {

  private static final String TRAILING_WILDCARD = "C+%";
  private static final String LEADING_WILDCARD = "%++";
  private static final String BOTH_SIDES_WILDCARD = "%+%";

  @Test
  public void testLeadingWildcard() {
    String result = LikeToRegexFormatConverterUtil.processValue(LEADING_WILDCARD);

    assert result.equals(".*\\+\\+");
  }

  @Test
  public void testTrailingWildcard() {
    String result = LikeToRegexFormatConverterUtil.processValue(TRAILING_WILDCARD);

    assert result.equals("C\\+.*");
  }

  @Test
  public void testBothSidesWildcard() {
    String result = LikeToRegexFormatConverterUtil.processValue(BOTH_SIDES_WILDCARD);

    assert result.equals(".*\\+.*");
  }
}
