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
package org.apache.pinot.common.utils;

import java.util.List;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RoaringBitmapUtilsTest {

  @Test
  public void testFromInclusiveRangesWithoutRanges() {
    assertTrue(RoaringBitmapUtils.fromInclusiveRanges(List.of()).isEmpty());
  }

  @Test
  public void testFromInclusiveRangesWithSingleRange() {
    assertEquals(RoaringBitmapUtils.fromInclusiveRanges(List.of(new IntPair(3, 5))).toArray(), new int[]{3, 4, 5});
  }

  @Test
  public void testFromInclusiveRangesWithMultipleRanges() {
    // Unsorted, overlapping and adjacent ranges that share a container, and one that crosses into the next container
    List<IntPair> ranges = List.of(
        new IntPair(65535, 65536),
        new IntPair(3, 5),
        new IntPair(4, 6),
        new IntPair(7, 7),
        new IntPair(0, 0)
    );
    assertEquals(RoaringBitmapUtils.fromInclusiveRanges(ranges).toArray(), new int[]{0, 3, 4, 5, 6, 7, 65535, 65536});
  }
}
