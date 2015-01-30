package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Utils class.
 *
 * @author jfim
 */
public class UtilsTest {
  @Test
  public void testToCamelCase() {
    Assert.assertEquals(Utils.toCamelCase("Hello world!"), "HelloWorld");
    Assert.assertEquals(Utils.toCamelCase("blah blah blah"), "blahBlahBlah");
    Assert.assertEquals(Utils.toCamelCase("the quick __--???!!! brown   fox?"), "theQuickBrownFox");
  }
}
