/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Utils class.
 *
 */
public class UtilsTest {
  @Test
  public void testToCamelCase() {
    Assert.assertEquals(Utils.toCamelCase("Hello world!"), "HelloWorld");
    Assert.assertEquals(Utils.toCamelCase("blah blah blah"), "blahBlahBlah");
    Assert.assertEquals(Utils.toCamelCase("the quick __--???!!! brown   fox?"), "theQuickBrownFox");
  }
}
