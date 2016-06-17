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
package com.linkedin.pinot.core.indexsegment;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for IndexType.
 *
 */
public class IndexTypeTest {
  @Test
  public void testValueOfStr() {
    Assert.assertEquals(IndexType.valueOfStr("columnar"), IndexType.COLUMNAR);
    Assert.assertEquals(IndexType.valueOfStr("simple"), IndexType.SIMPLE);

    try {
      IndexType.valueOfStr("this does not exist");

      // Should throw an exception
      Assert.fail();
    } catch (Exception e) {
      // Success!
    }
  }
}
