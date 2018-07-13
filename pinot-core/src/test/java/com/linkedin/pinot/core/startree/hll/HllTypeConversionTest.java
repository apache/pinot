/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.hll;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;


/**
 * Test Conversion between Hll and String type
 */
public class HllTypeConversionTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllTypeConversionTest.class);
  private static Charset charset = Charset.forName("UTF-8");
  private Random rand = new Random();

  @Test
  public void testConvertInByteRange()
      throws Exception {
    int max = Byte.MAX_VALUE;
    int min = Byte.MIN_VALUE;

    for (long i = min; i <= max; i++) { // use long type to prevent overflow
      Assert.assertEquals(HllUtil.SerializationConverter.charToByte(
          new String(new char[]{HllUtil.SerializationConverter.byteToChar((byte) i)}).toCharArray()[0]), i);
    }
  }

  private void testConvertArraySize(int arraySize) {
    byte[] byteArray = new byte[arraySize];
    rand.nextBytes(byteArray);
    String s = new String(HllUtil.SerializationConverter.byteArrayToChars(byteArray));

    byte[] byteArray2 =
        HllUtil.SerializationConverter.charsToByteArray(new String(s.getBytes(charset), charset).toCharArray());
    Assert.assertTrue(Arrays.equals(byteArray, byteArray2));
  }

  @Test
  public void testToStringFunctionality() {
    int numOfTest = 100000;
    int maxArraySize = 50;

    for (int i = 0; i < maxArraySize; i++) {
      LOGGER.info("Test Size: " + i);
      for (int k = 0; k < numOfTest; k++) {
        testConvertArraySize(i);
      }
    }
  }
}
