/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link StringUtil} class.
 */
public class StringUtilTest {

  @Test
  public void testContainsNullCharacter() {
    Assert.assertFalse(StringUtil.containsNullCharacter(""));
    Assert.assertFalse(StringUtil.containsNullCharacter("potato"));
    Assert.assertTrue(StringUtil.containsNullCharacter("\0"));
    Assert.assertTrue(StringUtil.containsNullCharacter("pot\0ato"));
  }

  @Test
  public void testRemoveNullCharacters() {
    Assert.assertEquals(StringUtil.removeNullCharacters(""), "");
    Assert.assertEquals(StringUtil.removeNullCharacters("potato"), "potato");
    Assert.assertEquals(StringUtil.removeNullCharacters("\0"), "");
    Assert.assertEquals(StringUtil.removeNullCharacters("pot\0ato"), "potato");
  }
}
