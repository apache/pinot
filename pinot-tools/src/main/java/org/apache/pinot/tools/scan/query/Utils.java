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
package org.apache.pinot.tools.scan.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Utils {
  private Utils() {
  }

  public static List<List<Object>> cartesianProduct(List<List<Object>> lists) {
    int numElements = 1;
    for (int i = 0; i < lists.size(); i++) {
      numElements *= lists.get(i).size();
    }

    List<List<Object>> result = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      int j = 1;

      List<Object> oneSet = new ArrayList<>();
      for (List<Object> list : lists) {
        int index = (i / j) % list.size();
        oneSet.add(list.get(index));
        j *= list.size();
      }
      result.add(oneSet);
    }
    return result;
  }

  public static void main(String[] args) {
    List<List<Object>> test = new ArrayList<>();
    List<Object> list1 = new ArrayList<Object>();
    list1.addAll(Arrays.asList(1, 2, 3));
    test.add(list1);
    List<Object> list2 = new ArrayList<Object>();
    list2.addAll(Arrays.asList(9));
    test.add(list2);
    List<Object> list3 = new ArrayList<Object>();
    list3.addAll(Arrays.asList(4, 5));
    test.add(list3);

    for (List<Object> list : cartesianProduct(test)) {
      for (Object object : list) {
        System.out.print(object + " ");
      }
      System.out.println();
    }
  }
}
