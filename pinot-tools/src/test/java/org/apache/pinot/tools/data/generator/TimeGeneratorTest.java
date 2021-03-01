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

package org.apache.pinot.tools.data.generator;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeGeneratorTest {

  @Test
  public void testConvert() {
    int oneHourInMillis = 60 * 60 * 1000;
    Date date = new Date(oneHourInMillis + 1);

    // seconds
    Number convertedTime = TimeGenerator.convert(date, TimeUnit.SECONDS, FieldSpec.DataType.LONG);
    assertTrue(convertedTime instanceof Long);
    assertEquals(3600L, convertedTime);

    // minutes
    convertedTime = TimeGenerator.convert(date, TimeUnit.MINUTES, FieldSpec.DataType.INT);
    assertTrue(convertedTime instanceof Integer);
    assertEquals(60, convertedTime);

    // check hours
    convertedTime = TimeGenerator.convert(date, TimeUnit.HOURS, FieldSpec.DataType.INT);
    assertTrue(convertedTime instanceof Integer);
    assertEquals(1, convertedTime);
  }
}
