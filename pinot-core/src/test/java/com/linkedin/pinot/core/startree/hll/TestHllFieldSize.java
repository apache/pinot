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
package com.linkedin.pinot.core.startree.hll;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;


public class TestHllFieldSize {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHllFieldSize.class);
  private Random rand = new Random();

  private static int calculateHllSerializedSizeInBytes(int log2m) {
    try {
      return new HyperLogLog(log2m).getBytes().length;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Test
  public void verifyHllConstantsSize() {
    // verify size
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int key: HllUtil.LOG2M_TO_SIZE_IN_BYTES.keySet()) {
      if (key > max) max = key;
      if (key < min) min = key;
    }
    for (int log2m = min; log2m <= max; log2m++) {
      int realSize = calculateHllSerializedSizeInBytes(log2m);
      LOGGER.info("Log2m {}: SerializedSize {}", log2m, realSize);
      Assert.assertEquals(realSize, HllUtil.getHllFieldSizeFromLog2m(log2m));
    }
  }

  @Test
  public void testHllFieldSerializedSize()
      throws Exception {
    for (int i = 5; i < 10; i++) {
      HyperLogLog hll = new HyperLogLog(i);
      Assert.assertEquals(HllUtil.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
      for (int j = 0; j < 100; j++) {
        hll.offer(rand.nextLong());
      }
      Assert.assertEquals(HllUtil.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
      for (int j = 0; j < 9900; j++) {
        hll.offer(rand.nextLong());
      }
      Assert.assertEquals(HllUtil.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
    }
  }
}
