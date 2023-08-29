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
import java.math.RoundingMode;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BigDecimalUtilsTest {

  @Test
  public void testBigDecimal() {
    BigDecimal[] testCases = {
        new BigDecimal("0.123456789"),
        new BigDecimal("-0.123456789"),
        new BigDecimal("123456789"),
        new BigDecimal("-123456789"),
        new BigDecimal("123456789.0123456789"),
        new BigDecimal("-123456789.0123456789"),
        // Set the scale to a negative value
        new BigDecimal("123456789.0123456789").setScale(-1, RoundingMode.HALF_UP),
        new BigDecimal("-123456789.0123456789").setScale(-1, RoundingMode.HALF_UP),
        // Set the scale to a negative value in byte
        new BigDecimal("123456789.0123456789").setScale(128, RoundingMode.HALF_UP),
        new BigDecimal("-123456789.0123456789").setScale(128, RoundingMode.HALF_UP)
    };
    for (BigDecimal value : testCases) {
      byte[] serializedValue = BigDecimalUtils.serialize(value);
      assertEquals(BigDecimalUtils.byteSize(value), serializedValue.length);
      BigDecimal deserializedValue = BigDecimalUtils.deserialize(serializedValue);
      assertEquals(deserializedValue, value);
    }
  }

  @Test
  public void testBigDecimalSerializeWithSize() {
    BigDecimal[] testCases = {
        new BigDecimal("0.123456789"),
        new BigDecimal("-0.123456789"),
        new BigDecimal("123456789"),
        new BigDecimal("-123456789"),
        new BigDecimal("123456789.0123456789"),
        new BigDecimal("-123456789.0123456789"),
        new BigDecimal("123456789.0123456789").setScale(-1, RoundingMode.HALF_UP),
        new BigDecimal("-123456789.0123456789").setScale(-1, RoundingMode.HALF_UP),
        new BigDecimal("123456789.0123456789").setScale(128, RoundingMode.HALF_UP),
        new BigDecimal("-123456789.0123456789").setScale(128, RoundingMode.HALF_UP)
    };
    // One case of serialization with and without padding
    int[] sizes = {0, 4};
    for (BigDecimal value : testCases) {
      int actualSize = BigDecimalUtils.byteSize(value);
      for (int size : sizes) {
        byte[] serializedValue = BigDecimalUtils.serializeWithSize(value, actualSize + size);
        assertEquals(actualSize + size, serializedValue.length);
        BigDecimal deserializedValue = BigDecimalUtils.deserialize(serializedValue);
        assertEquals(deserializedValue, value);
      }
    }
  }

  @Test
  public void testGenerateMaximumNumberWithPrecision() {
    int[] testCases = { 1, 3, 10, 38, 128 };
    for (int precision : testCases) {
      BigDecimal bd = BigDecimalUtils.generateMaximumNumberWithPrecision(precision);
      assertEquals(bd.precision(), precision);
      assertEquals(bd.add(new BigDecimal("1")).precision(), precision + 1);
    }
  }

  @Test
  public void testBigDecimalWithMaximumPrecisionSizeInBytes() {
    Assert.assertEquals(BigDecimalUtils.byteSizeForFixedPrecision(18), 10);
    Assert.assertEquals(BigDecimalUtils.byteSizeForFixedPrecision(32), 16);
    Assert.assertEquals(BigDecimalUtils.byteSizeForFixedPrecision(38), 18);
  }

  @Test
  public void testBigDecimalSerializationWithSize() {
    ArrayList<BigDecimal> bigDecimals = new ArrayList<>();
    bigDecimals.add(new BigDecimal("1000.123456"));
    bigDecimals.add(new BigDecimal("1237663"));
    bigDecimals.add(new BigDecimal("0.114141622"));

    for (BigDecimal bigDecimal : bigDecimals) {
      int bytesNeeded = BigDecimalUtils.byteSize(bigDecimal);

      // Serialize big decimal equal to the target size
      byte[] bytes = BigDecimalUtils.serializeWithSize(bigDecimal, bytesNeeded);
      BigDecimal bigDecimalDeserialized = BigDecimalUtils.deserialize(bytes);
      Assert.assertEquals(bigDecimalDeserialized, bigDecimal);

      // Serialize big decimal smaller than the target size
      bytes = BigDecimalUtils.serializeWithSize(bigDecimal, bytesNeeded + 2);
      bigDecimalDeserialized = BigDecimalUtils.deserialize(bytes);
      Assert.assertEquals(bigDecimalDeserialized, bigDecimal);

      // Raise exception when trying to serialize a big decimal larger than target size
      Assert.assertThrows(IllegalArgumentException.class, () -> {
        BigDecimalUtils.serializeWithSize(bigDecimal, bytesNeeded - 4);
      });
    }
  }
}
