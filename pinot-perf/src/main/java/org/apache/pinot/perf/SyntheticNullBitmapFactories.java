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
package org.apache.pinot.perf;

import java.util.Random;
import java.util.function.IntSupplier;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.roaringbitmap.RoaringBitmap;


/**
 * Synthetic null bitmap suppliers for testing and benchmarking.
 */
public class SyntheticNullBitmapFactories {
  private SyntheticNullBitmapFactories() {
  }

  /**
   * Null bitmap factories that generate null bitmaps with periodic patterns.
   */
  public static class Periodic {
    private Periodic() {
    }

    /**
     * Returns a null bitmap with the first doc in each period set as null.
     */
    public static RoaringBitmap firstInPeriod(int numDocs, int period) {
      return periodic(numDocs, period, () -> 0);
    }

    /**
     * Returns a null bitmap with the last doc in each period set as null.
     */
    public static RoaringBitmap lastInPeriod(int numDocs, int period) {
      return periodic(numDocs, period, () -> period - 1);
    }

    /**
     * Returns a null bitmap with a random doc in each period set as null.
     */
    public static RoaringBitmap randomInPeriod(int numDocs, int period) {
      Random random = new Random(42);
      return randomInPeriod(numDocs, period, random);
    }

    /**
     * Returns a null bitmap with a random doc in each period set as null.
     */
    public static RoaringBitmap randomInPeriod(int numDocs, int period, Random random) {
      return periodic(numDocs, period, () -> random.nextInt(period));
    }

    /**
     * Returns a null bitmap with a doc in each period set as null, with the doc position in the period determined by
     * the given supplier.
     *
     * @param inIntervalSupplier Supplier for the position of the doc in the period.
     *                           The supplier should return a value in the range {@code [0, period)}.
     */
    public static RoaringBitmap periodic(int numDocs, int period, IntSupplier inIntervalSupplier) {
      RoaringBitmap nullBitmap = new RoaringBitmap();
      for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i += period) {
        int pos = i + inIntervalSupplier.getAsInt();
        if (pos < numDocs) {
          nullBitmap.add(pos);
        }
      }
      nullBitmap.runOptimize();
      return nullBitmap;
    }
  }
}
