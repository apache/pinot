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

package org.apache.pinot.controller.recommender.data.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;


/**
 * A helper class for generating multi value entries
 */
public class MultiValueGeneratorHelper {

  /**
   * Generate MV entries
   *
   * @param numberOfValuesPerEntry number of values per each row
   * @param rand random object
   * @param nextItemFunc function to get the next random item
   * @return
   */
  public static List<Object> generateMultiValueEntries(double numberOfValuesPerEntry, Random rand,
      Supplier<Object> nextItemFunc) {
    List<Object> entries = new ArrayList<>();
    int i = 0;
    for (; i < numberOfValuesPerEntry - 1; i++) {
      entries.add(nextItemFunc.get());
    }
    // last item
    if (rand.nextDouble() < numberOfValuesPerEntry - i) {
      entries.add(nextItemFunc.get());
    }
    return entries;
  }
}
