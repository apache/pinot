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
package org.apache.pinot.spi.utils;

import java.math.BigDecimal;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BigDecimalUtilsTest {

  @Test
  public void testBigDecimal() {
    BigDecimal value = new BigDecimal("123456789.0123456789");
    byte[] serializedValue = BigDecimalUtils.serialize(value);
    assertEquals(BigDecimalUtils.byteSize(value), serializedValue.length);
    BigDecimal deserializedValue = BigDecimalUtils.deserialize(serializedValue);
    assertEquals(deserializedValue, value);

    // Set the scale to a negative value in byte
    value = value.setScale(128, BigDecimal.ROUND_UNNECESSARY);
    serializedValue = BigDecimalUtils.serialize(value);
    assertEquals(BigDecimalUtils.byteSize(value), serializedValue.length);
    deserializedValue = BigDecimalUtils.deserialize(serializedValue);
    assertEquals(deserializedValue, value);
  }

  @Test
  public void testToBigDecimal() {
    BigDecimal value = BigDecimalUtils.toBigDecimal("123.4567");
    assertEquals(value.precision(), 7);
    assertEquals(value.scale(), 4);
    assertEquals(value.intValue(), 123);
    assertEquals(value.remainder(BigDecimal.ONE), BigDecimal.valueOf(0.4567));
  }

  @Test
  public void testReferenceMinValue() {
    BigDecimal value = BigDecimalUtils.referenceMinValue(4);
    assertEquals(value.precision(), 313); // small precisions are ignored if the number doesn't fit
    assertEquals(value.scale(), 4);
    assertEquals(value.signum(), -1);
  }

  @Test
  public void testCreateBigDecimal() {
    BigDecimal value = BigDecimalUtils.createBigDecimal("123.123456", 14, 4);
    // the specified precision is greater than the precision needed
    assertEquals(value.precision(), 7);
    assertEquals(value.scale(), 4);
    assertEquals(value.intValue(), 123);
    // round up if scale is smaller than the number of decimal digits
    assertEquals(value.remainder(BigDecimal.ONE), BigDecimal.valueOf(0.1235));
  }
}
