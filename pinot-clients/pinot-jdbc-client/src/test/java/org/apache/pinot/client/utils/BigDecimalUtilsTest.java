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
package org.apache.pinot.client.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BigDecimalUtilsTest {

  @Test
  public void testGetCalculatedScale() {
    int calculatedResult;

    calculatedResult = BigDecimalUtils.getCalculatedScale("1");
    Assert.assertEquals(calculatedResult, 0);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.0");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.2");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.23");
    Assert.assertEquals(calculatedResult, 2);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.234");
    Assert.assertEquals(calculatedResult, 3);

    calculatedResult = BigDecimalUtils.getCalculatedScale("-1.234");
    Assert.assertEquals(calculatedResult, 3);
  }
}
