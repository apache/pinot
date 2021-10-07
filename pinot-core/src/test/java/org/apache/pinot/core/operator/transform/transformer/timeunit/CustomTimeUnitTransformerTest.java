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
package org.apache.pinot.core.operator.transform.transformer.timeunit;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class CustomTimeUnitTransformerTest {

  @Test
  public void testTransform() {
    CustomTimeUnitTransformer transformer = new CustomTimeUnitTransformer(TimeUnit.SECONDS, "WEEKS");
    long[] input = new long[]{ 3543535L, 8432948689374L, 23274678L };
    long[] output = new long[input.length];
    transformer.transform(input, output, input.length);
    assertEquals(output, new long[]{5, 13943367, 38});

    transformer = new CustomTimeUnitTransformer(TimeUnit.DAYS, "MONTHS");
    input = new long[]{ 32L, 365L, 98L };
    output = new long[input.length];
    transformer.transform(input, output, input.length);
    assertEquals(output, new long[]{1, 12, 3});

    transformer = new CustomTimeUnitTransformer(TimeUnit.DAYS, "YEARS");
    input = new long[]{ 32L, 365L, 3150L };
    output = new long[input.length];
    transformer.transform(input, output, input.length);
    assertEquals(output, new long[]{0, 1, 8});

    assertThrows(IllegalArgumentException.class, () -> new CustomTimeUnitTransformer(TimeUnit.DAYS, "SECONDS"));
  }
}
