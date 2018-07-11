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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.startree.hll.HllSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;


public class HllFieldSizeTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllFieldSizeTest.class);
  private Random rand = new Random();

  @Test
  public void testHllFieldSerializedSize()
      throws Exception {
    for (int i = 5; i < 10; i++) {
      HyperLogLog hll = new HyperLogLog(i);
      Assert.assertEquals(HllSizeUtils.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
      for (int j = 0; j < 100; j++) {
        hll.offer(rand.nextLong());
      }
      Assert.assertEquals(HllSizeUtils.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
      for (int j = 0; j < 9900; j++) {
        hll.offer(rand.nextLong());
      }
      Assert.assertEquals(HllSizeUtils.getHllFieldSizeFromLog2m(i), hll.getBytes().length);
      LOGGER.info("Estimated: " + hll.cardinality());
    }
  }
}
